/*
 * Copyright 2019 The JIMDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.jimdb.engine.sender;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.engine.client.JimCommand;
import io.jimdb.engine.client.JimCommandDecoder;
import io.jimdb.engine.client.JimHeartbeat;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.common.exception.ConnectException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.meta.RouterManager;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.pb.Api;
import io.jimdb.pb.Api.RangeRequest.ReqCase;
import io.jimdb.pb.Errorpb;
import io.jimdb.pb.Function.FunctionID;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Statspb;
import io.jimdb.pb.Txn;
import io.jimdb.rpc.client.NettyClient;
import io.jimdb.rpc.client.command.CommandCallback;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * ShardSender sends the wrapped request to RPC and handles any errors that may occur
 *
 */
@SuppressFBWarnings({"CE_CLASS_ENVY", "CC_CYCLOMATIC_COMPLEXITY"})
public final class ShardSender implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSender.class);

  private static final short FUNC_ID = FunctionID.kFuncRangeRequest_VALUE;

  private final RouterManager routeManager;
  private final NettyClient client;
  private final ExecutorService retryThreadPool;
  private final ScheduledExecutorService retryExecutor;

  public ShardSender(final JimConfig config, final RouterManager routeManager) {
    NettyClientConfig clientConfig = config.getClientConfig();
    Preconditions.checkArgument(clientConfig != null, "client config must not be null");
    this.routeManager = routeManager;
    this.client = new NettyClient(clientConfig, JimCommand::new, () -> {
      return new JimCommandDecoder(clientConfig.getFrameMaxSize(), JimCommand.LENGTH_FIELD_OFFSET, JimCommand.LENGTH_FIELD_SIZE);
    }, new JimHeartbeat());
    this.retryThreadPool = config.getOutboundExecutor();
    Preconditions.checkArgument(retryThreadPool != null, "retryThreadPool must not be null");
    this.retryExecutor = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("Send-Retry-Executor", true));
  }

  protected void start() {
  }

  private void retryCommand(final RequestContext context, final CommandCallback<JimCommand> callback, Throwable err) {
    retryThreadPool.execute(() -> {
      long delay = context.retryDelay();
      if (delay > 0) {
        LOG.warn("cxtId {} is not timeout, cmdType:{}, instant: {}, retry, err{}", context.getCtxId(),
                context.getCmdType(), context.getInstant(), err);
        retryExecutor.schedule(() -> {
          context.refreshRangeInfo();
          this.sendCommand(context, callback);
        }, delay, TimeUnit.MILLISECONDS);
      } else {
        LOG.error("ctxId {} execute timeout, cmdType:{}, internal err: {}", context.getCtxId(), context.getCmdType(), err);
        Throwable exception = DBException.get(ErrorModule.ENGINE, ErrorCode.ER_CONTEXT_TIMEOUT, err.toString());
        callback.onFailed(null, exception);
      }
    });
  }

  public Flux<Message> sendReq(RequestContext context) {
    // send request
    Flux<Message> flux = Flux.create(sink -> {
      CommandCallback<JimCommand> callback = new CommandCallback<JimCommand>() {
        @Override
        protected boolean onSuccess0(JimCommand request, JimCommand response) {
          try {
            if (request.getFuncId() == null || response.getFuncId() == null
                    || request.getFuncId().intValue() != response.getFuncId().intValue()) {
              LOG.warn("ctxId {} request {} response {} funcId is not equal.",
                      context.getCtxId(), request, response);
            }

            Api.RangeResponse rpcResponse = (Api.RangeResponse) response.getBody();
            Api.RangeResponse.HeaderOrBuilder headerBuilder = rpcResponse.getHeaderOrBuilder();
            if (headerBuilder.hasError()) {
              Errorpb.Error error = headerBuilder.getError();
              ErrorHandler.handleRangeErr(error, context, response.getReqID());
              retryCommand(context, this, DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_ERROR,
                      String.valueOf(response.getReqID()), error.toString()));
              return false;
            }

            Message message = parseResponse(rpcResponse, context.getCmdType());
            if (LOG.isInfoEnabled()) {
              LOG.info("ctxId {} cmdType {} requestId {} success", context.getCtxId(), context.getCmdType(), response.getReqID());
            }
            sink.next(message);
          } catch (Throwable throwable) {
            onFailed0(request, throwable);
          }
          return true;
        }

        @Override
        protected boolean onFailed0(JimCommand request, Throwable cause) {
          try {
            handleError(context, cause, this);
          } catch (Throwable e) {
            if (!(e instanceof BaseException)) {
              LOG.warn("ctx {} send unknown error {}", context.getCtxId(), e);
              e = DBException.get(ErrorModule.ENGINE, ErrorCode.ER_UNKNOWN_ERROR, e);
            }
            sink.error(e);
          }
          return true;
        }
      };
      sink.onRequest(n -> sendCommand(context, callback));
    });

    return flux.doFinally(t -> {
      if (LOG.isInfoEnabled()) {
        LOG.info("Source get finally type:{} ", t);
      }
    }).onErrorResume(error -> {
      LOG.error("Source get {} error:{} ", context.getKey(), error);
      return Flux.error(error);
    });
  }

  // handle err
  private void handleError(RequestContext context, Throwable err, CommandCallback callback) throws Throwable {
    if (!(err instanceof BaseException)) {
      LOG.warn("ctxId {} directly throw err{}", context.getCtxId(), err);
      throw err;
    }

    ErrorCode code = ((BaseException) err).getCode();
    switch (code) {
      case ER_CONTEXT_TIMEOUT:
      case ER_SHARD_NOT_EXIST:
      case ER_SHARD_ROUTE_CHANGE:
        //no retry at shard sender
        LOG.warn("ctxId {} directly throw err{}", context.getCtxId(), err);
        throw err;
        //todo update to throw
      case ER_BAD_DB_ERROR:
      case ER_BAD_TABLE_ERROR:
//        changeMeta(context, metaData, err, callback);
        return;
//        case ER_META_NODE_NOT_EXIST:
//        case ER_META_SHARD_NOT_EXIST:
      case ER_RPC_REQUEST_TIMEOUT:
        //todo need to stat
        clearRoute(context);
        break;
      case ER_RPC_CONNECT_INACTIVE:
      case ER_RPC_CONNECT_TIMEOUT:
      case ER_RPC_CONNECT_UNKNOWN:
      case ER_RPC_CONNECT_INTERRUPTED:
      case ER_RPC_CONNECT_ERROR:
      case ER_RPC_REQUEST_CODEC:
      case ER_RPC_REQUEST_INVALID:
      case ER_RPC_REQUEST_ERROR:
        if (err instanceof ConnectException) {
          LOG.warn("ctxId {} retry for ConnectionException:{}", context.getCtxId(), err.getMessage());
          //retry
          clearRoute(context);
        }
        break;
      default:
        break;
    }
    this.retryCommand(context, callback, err);
  }

  private void clearRoute(RequestContext context) {
    RoutePolicy routePolicy = context.getRoutePolicy();
    if (routePolicy != null) {
      routePolicy.removeRangeByKey(context.getKey().toByteArray(), null);
    }
  }

//  private void changeMeta(RequestContext context, MetaData metaData, Throwable err, CommandCallback callback) {
//    Table oldTable = context.getTable();
//    MetaData metaDataNew = this.metaEngine.getMetaData();
//    if (metaDataNew != null) {
//      Table cacheTable = metaDataNew.getTable(oldTable.getCatalog().getName(), oldTable.getName());
//      if (cacheTable != null && (cacheTable.getDbId() != oldTable.getDbId() || cacheTable.getId() != oldTable.getId())) {
//        LOG.warn("ctxId {} found more new table from local, old table id:{}, new table id:{}, update and retry.",
//                context.getCtxId(), oldTable.getId(), cacheTable.getId());
//        this.routeManager.removePolicy(oldTable.getId());
//        context.setRoutePolicy(this.routeManager.getOrCreatePolicy(cacheTable.getDbId(), cacheTable.getId()));
//        context.setTable(cacheTable);
////        ByteString key = context.getKey().toByteArray();
////        context.setKey();
//        retryCommand(context, callback, err);
//        return;
//      }
//    }
//
//    CompletableFuture<?> syncFuture = this.metaEngine.sync(metaData == null ? 0 : metaData.getVersion());
//    syncFuture.whenCompleteAsync((result, e) -> {
//      if (e != null) {
//        LOG.error("ctxId {} sync meta data error:{}.", context.getCtxId(), e);
//      }
//      MetaStore.MetaData metaDataNewN = this.metaEngine.getMetaData();
//      if (metaDataNewN == null) {
//        LOG.warn("ctxId {} metaDataNewN is null, retry.", context.getCtxId());
//      } else {
//        Table cacheTable = metaDataNewN.getTable(oldTable.getCatalog().getName(), oldTable.getName());
//        if (cacheTable == null) {
//          LOG.warn("ctxId {} metaDataNewN is not null, table {} not exist, retry. ", context.getCtxId(), oldTable.getName());
//        } else {
//          if (cacheTable.getDbId() != oldTable.getDbId() || cacheTable.getId() != oldTable.getId()) {
//            this.routeManager.removePolicy(oldTable.getId());
//            context.setRoutePolicy(this.routeManager.getOrCreatePolicy(cacheTable.getDbId(), cacheTable.getId()));
//            LOG.warn("[sync]ctxId {} found more new table from local, old table id:{}, new table id:{}, update and retry.",
//                    context.getCtxId(), oldTable.getId(), cacheTable.getId());
//          }
//          context.setTable(cacheTable);
//        }
//      }
//      //todo schema online dynamic change
//      retryCommand(context, callback, err);
//    });
//  }

  @Override
  @SuppressFBWarnings("HES_EXECUTOR_NEVER_SHUTDOWN")
  public void close() {
    if (retryThreadPool != null) {
      this.retryThreadPool.shutdown();
    }
  }

  private void sendCommand(final RequestContext context, final CommandCallback<JimCommand> callback) {
    try {
      final RangeInfo rangeInfo = context.getRangeInfo();
      if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
        LOG.warn("ctx {} locate key {} error: range info is {} ", context.getCtxId(),
                Arrays.toString(NettyByteString.asByteArray(context.getKey())), rangeInfo == null ? "null" : rangeInfo.getId());
        throw RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, NettyByteString.asByteArray(context.getKey()));
      }
      checkRangeBound(context, rangeInfo);
      Message msgBody = getMsgBody(context, rangeInfo, context.getReqBuilder());
      if (LOG.isDebugEnabled()) {
        LOG.debug("sending request: {}", context.getReqBuilder());
      }
      JimCommand command = new JimCommand(FUNC_ID, msgBody);
      this.client.send(rangeInfo.getLeaderAddr(), command, callback);
    } catch (Exception ex) {
      if (callback != null) {
        callback.onFailed(null, ex);
      } else {
        throw ex;
      }
    }
  }

  private void checkRangeBound(RequestContext context, RangeInfo rangeInfo) {
    ByteString key = context.getKey();
    if (ByteUtil.compare(rangeInfo.getStartKey(), key) > 0 || ByteUtil.compare(rangeInfo.getEndKey(), key) <= 0) {
      LOG.error("ctx {} locate key error,  key {}, range {}, request:{}",
              context.getCtxId(), Arrays.toString(NettyByteString.asByteArray(key)), rangeInfo, context.getReqBuilder());
    }
  }

  private static Api.RangeRequest.Header.Builder getMsgHeader(long clusterId, RangeInfo range) {
    Api.RangeRequest.Header.Builder headerBuilder = Api.RangeRequest.Header.newBuilder();
    headerBuilder.setClusterId(clusterId);
    headerBuilder.setRangeId(range.getId());
    headerBuilder.setRangeEpoch(range.getRangeEpoch());
    return headerBuilder;
  }

  private static Message getMsgBody(RequestContext context, RangeInfo range, Message.Builder reqBuilder) {

    Api.RangeRequest.Builder bodyBuilder = Api.RangeRequest.newBuilder()
            .setHeader(getMsgHeader(context.getClusterId(), range));

    ReqCase cmdType = context.getCmdType();
    switch (cmdType) {
      case KV_GET:
        bodyBuilder.setKvGet((Kv.KvGetRequest.Builder) reqBuilder);
        break;
      case KV_PUT:
        bodyBuilder.setKvPut((Kv.KvPutRequest.Builder) reqBuilder);
        break;
      case KV_DELETE:
        bodyBuilder.setKvDelete((Kv.KvDeleteRequest.Builder) reqBuilder);
        break;
      case PREPARE:
        bodyBuilder.setPrepare((Txn.PrepareRequest.Builder) reqBuilder);
        break;
      case DECIDE:
        bodyBuilder.setDecide((Txn.DecideRequest.Builder) reqBuilder);
        break;
      case CLEAR_UP:
        bodyBuilder.setClearUp((Txn.ClearupRequest.Builder) reqBuilder);
        break;
      case GET_LOCK_INFO:
        bodyBuilder.setGetLockInfo((Txn.GetLockInfoRequest.Builder) reqBuilder);
        break;
      case SELECT:
        bodyBuilder.setSelect((Txn.SelectRequest.Builder) reqBuilder);
        break;
      case SCAN:
        bodyBuilder.setScan((Txn.ScanRequest.Builder) reqBuilder);
        break;
      case SELECT_FLOW:
        bodyBuilder.setSelectFlow((Txn.SelectFlowRequest.Builder) reqBuilder);
        break;
      case INDEX_STATS:
        bodyBuilder.setIndexStats((Statspb.IndexStatsRequest.Builder) reqBuilder);
        break;
      case COLUMNS_STATS:
        bodyBuilder.setColumnsStats((Statspb.ColumnsStatsRequest.Builder) reqBuilder);
        break;
      default:
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "command type:" + cmdType);
    }
    return bodyBuilder.build();
  }

  @SuppressFBWarnings("CE_CLASS_ENVY")
  private static Message parseResponse(Api.RangeResponse rpcResponse, ReqCase cmdType) {
    Message message;
    switch (cmdType) {
      case KV_GET:
        message = rpcResponse.getKvGet();
        break;
      case KV_PUT:
        message = rpcResponse.getKvPut();
        break;
      case KV_DELETE:
        message = rpcResponse.getKvDelete();
        break;
      case PREPARE:
        message = rpcResponse.getPrepare();
        break;
      case DECIDE:
        message = rpcResponse.getDecide();
        break;
      case CLEAR_UP:
        message = rpcResponse.getClearUp();
        break;
      case GET_LOCK_INFO:
        message = rpcResponse.getGetLockInfo();
        break;
      case SELECT:
        message = rpcResponse.getSelect();
        break;
      case SCAN:
        message = rpcResponse.getScan();
        break;
      case SELECT_FLOW:
        message = rpcResponse.getSelectFlow();
        break;

      case INDEX_STATS:
        message = rpcResponse.getIdxStats();
        break;

      case COLUMNS_STATS:
        message = rpcResponse.getColsStats();
        break;

      default:
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "command type:" + cmdType);
    }
    return message;
  }
}
