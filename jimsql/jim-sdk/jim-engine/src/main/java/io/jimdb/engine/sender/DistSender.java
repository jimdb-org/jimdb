/*
 * Copyright 2019 The JimDB Authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.config.JimConfig;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.RangeRouteException;
import io.jimdb.meta.RouterManager;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.pb.Api.RangeRequest.ReqCase;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Statspb;
import io.jimdb.pb.Txn;
import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.NettyByteString;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Distributed request processing
 * involving multiple ShardSender requests
 *
 * @version V1.0
 */
public final class DistSender implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DistSender.class);

  private SelectRangeFunc<List<Txn.Row>, Txn.SelectRequest.Builder> selectRangeFunc = this::txnSingeSelect;
  private SelectRangeFunc<List<Txn.Row>, Txn.SelectFlowRequest.Builder> selectFlowRangeFunc = this::txnSingeSelectFlow;
  private SelectRangeFunc<List<Txn.KeyValue>, Txn.ScanRequest.Builder> scanRangeFunc = this::txnSingleScan;
  private SelectRangeFunc<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>, Statspb.IndexStatsRequest.Builder> singleAnalyzeIndexFunc = this::singleAnalyzeIndex;
  private SelectRangeFunc<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>, Statspb.ColumnsStatsRequest.Builder> singleAnalyzeColumnsFunc = this::singleAnalyzeColumns;

  private static final int ASYNC_QUEUE_NUM = 1000000;
  private static final int ASYNC_THREAD_NUM = 4;

  private final BlockingQueue<Runnable> asyncQueue;
  private final ExecutorService asyncExecutorPool;
  private final ShardSender shardSender;
  private volatile boolean isRunning = true;

  public DistSender(JimConfig config, RouterManager routeManager) {
    this.shardSender = new ShardSender(config, routeManager);
    this.asyncQueue = new LinkedBlockingQueue<>(ASYNC_QUEUE_NUM);
    this.asyncExecutorPool = new ThreadPoolExecutor(ASYNC_THREAD_NUM, ASYNC_THREAD_NUM, 50L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("DistSender-Asyncer", true));
  }

  public void start() {
    this.shardSender.start();
    asyncTask();
  }

  private void asyncTask() {
    for (int i = 0; i < ASYNC_THREAD_NUM; i++) {
      this.asyncExecutorPool.execute(() -> {
        while (isRunning) {
          try {
            if (LOG.isInfoEnabled()) {
              LOG.info("take task from queue");
            }
            Runnable task = asyncQueue.take();
            task.run();
          } catch (InterruptedException e1) {
            LOG.warn("take task from queue interrupted");
          } catch (Throwable e2) {
            LOG.error("take task from queue err", e2);
          }
        }
      });
    }
  }

  /**
   * raw put
   *
   * @param storeCtx
   * @param kvPair
   * @return
   */
  public Flux<Boolean> rawPut(StoreCtx storeCtx, KvPair kvPair) {
    Message.Builder rawPutBuilder = Util.buildRawPut(kvPair.getKey(), kvPair.getValue());
    RequestContext context = new RequestContext(storeCtx, kvPair.getKey(), rawPutBuilder, ReqCase.KV_PUT);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvPutResponse resp = (Kv.KvPutResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawPut",
                String.valueOf(resp.getCode()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("raw put response success: {}", resp);
      }
      return Boolean.TRUE;
    });
  }

  /**
   * raw get
   *
   * @param storeCtx
   * @param key
   * @return
   */
  public Flux<byte[]> rawGet(StoreCtx storeCtx, byte[] key) {
    ByteString bytes = NettyByteString.wrap(key);
    RequestContext context = new RequestContext(storeCtx, bytes, Util.buildRawGet(bytes), ReqCase.KV_GET);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvGetResponse resp = (Kv.KvGetResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawGet",
                String.valueOf(resp.getCode()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("raw get response success: {}", resp);
      }
      return NettyByteString.asByteArray(resp.getValue());
    });
  }

  /**
   * raw delete
   *
   * @param storeCtx
   * @param key
   * @return
   */
  public Flux<Boolean> rawDel(StoreCtx storeCtx, byte[] key) {
    ByteString bytes = NettyByteString.wrap(key);
    RequestContext context = new RequestContext(storeCtx, bytes, Util.buildRawDelete(bytes), ReqCase.KV_DELETE);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvDeleteResponse resp = (Kv.KvDeleteResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawDelete",
                String.valueOf(resp.getCode()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("raw del response success: {}", resp);
      }
      return Boolean.TRUE;
    });
  }

  public Flux<Txn.PrepareResponse> txnPrepare(RequestContext context) {
    return this.shardSender.sendReq(context).map(response -> (Txn.PrepareResponse) response);
  }

  public Flux<Txn.DecideResponse> txnDecide(RequestContext context) {
    return this.shardSender.sendReq(context).map(response -> (Txn.DecideResponse) response);
  }

  public Flux<Txn.ClearupResponse> clearUp(RequestContext context) {
    return this.shardSender.sendReq(context).map(response -> (Txn.ClearupResponse) response);
  }

  //Txn.SelectFlowRequest.Builder
  public Flux<List<Txn.Row>> txnSelectFlowForKeys(StoreCtx storeCtx, MessageOrBuilder reqBuilder, List<ByteString> keys) {
    return txnMultKeysSelectFlux(storeCtx, selectFlowRangeFunc, reqBuilder, keys);
  }

  //Txn.SelectFlowRequest.Builder
  public Flux<List<Txn.Row>> txnSelectFlowForRange(StoreCtx storeCtx, MessageOrBuilder reqBuilder, KvPair kvPair) {
    return txnRangeSelectFlux(storeCtx, selectFlowRangeFunc, reqBuilder, kvPair.getKey(), kvPair.getValue());
  }

  private Flux<List<Txn.Row>> txnSingeSelectFlow(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder,
                                                 ByteString key,
                                                       RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, ReqCase.SELECT_FLOW);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }
    return shardSender.sendReq(context).map(response -> {
      Txn.SelectFlowResponse selectResponse = (Txn.SelectFlowResponse) response;
      if (selectResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "singeSelect",
                String.valueOf(selectResponse.getCode()));
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("txn select response success: response row {}", selectResponse.getRowsList().size());
      }
//      LOG.error("txn select response success: response row {}", selectResponse.getTracesList());
      return selectResponse.getRowsList();
    });
  }

  private Flux<List<Txn.Row>> txnSingeSelectFlowStream(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder, ByteString key,
                                                       RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, ReqCase.SELECT_FLOW);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }
    return shardSender.sendReq(context).map(response -> {
      Txn.SelectFlowResponse selectResponse = (Txn.SelectFlowResponse) response;
      if (selectResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "singeSelect",
                String.valueOf(selectResponse.getCode()));
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("txn select response success: response row {}", selectResponse.getRowsList().size());
      }
      ByteString lastKey = selectResponse.getLastKey();
      if (!lastKey.isEmpty()) {
        reqBuilder.getProcessorsBuilder(0)
                .getTableReadBuilder().getRangeBuilder().setEndKey(selectResponse.getLastKey());
      }
      return selectResponse.getRowsList();
    });
  }

  public Flux<List<Txn.Row>> txnSelect(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder) {
    if (!reqBuilder.getKey().isEmpty()) {
      return txnSingeSelect(storeCtx, reqBuilder, reqBuilder.getKey(), null);
    } else {
      ByteString start = reqBuilder.getScope().getStart();
      ByteString end = reqBuilder.getScope().getLimit();
      return txnRangeSelectFlux(storeCtx, selectRangeFunc, reqBuilder, start, end);
    }
  }

  private <T> Flux<List<T>> txnMultKeysSelectFlux(StoreCtx storeCtx, SelectRangeFunc func,
                                                  MessageOrBuilder reqBuilder, List<ByteString> keys) {
    Flux<List<T>> flux = null;
    Collections.sort(keys, (k1, k2) -> Codec.compare(k1, k2));

    Map<RangeInfo, List<ByteString>> keyGroupMap;
    try {
      keyGroupMap = storeCtx.getRoutePolicy().regroupByRoute(keys, ByteString::toByteArray);
    } catch (Throwable e) {
      return Flux.error(e);
    }
    for (Map.Entry<RangeInfo, List<ByteString>> entry : keyGroupMap.entrySet()) {
      List<ByteString> keyList = entry.getValue();
      if (keyList == null || keyList.isEmpty()) {
        continue;
      }
      Flux<List<T>> selectFlux = func.apply(storeCtx, reqBuilder, keyList.get(0), entry.getKey());
      if (flux == null) {
        flux = selectFlux;
      } else {
        flux = flux.zipWith(selectFlux, (f1, f2) -> getRows(f1, f2));
      }
    }
    return flux;
  }

  private <T> Flux<List<T>> txnRangeSelectFlux(StoreCtx storeCtx, SelectRangeFunc func,
                                               MessageOrBuilder reqBuilder, ByteString start, ByteString end) {
    Flux<List<T>> flux = null;
    RoutePolicy routePolicy = storeCtx.getRoutePolicy();

    byte[] key = NettyByteString.asByteArray(start);
    RangeInfo rangeInfo;
    while (true) {
      rangeInfo = routePolicy.getRangeInfoByKey(key);
      if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
        LOG.error("locate route no exist by key {} for select, retry.", Arrays.toString(key));
        return Flux.error(RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key));
      }

      Flux<List<T>> selectFlux = func.apply(storeCtx, reqBuilder, NettyByteString.wrap(key), rangeInfo);
      if (flux == null) {
        flux = selectFlux;
      } else {
        flux = flux.zipWith(selectFlux, (f1, f2) -> getRows(f1, f2));
      }

      key = rangeInfo.getEndKey();
      if (Codec.compare(key, start) < 0 || Codec.compare(key, end) >= 0) {
        break;
      }
    }
    return flux;
  }

  private Flux<List<Txn.Row>> txnSingeSelect(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder, ByteString key,
                                             RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, ReqCase.SELECT);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }
    return shardSender.sendReq(context).map(response -> {
      Txn.SelectResponse selectResponse = (Txn.SelectResponse) response;
      if (selectResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "singeSelect",
                String.valueOf(selectResponse.getCode()));
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("txn select response success: column size {}, response row {}", reqBuilder.getFieldListCount(),
                selectResponse.getRowsList().size());
      }
      return selectResponse.getRowsList();
    });
  }

  public Flux<Txn.GetLockInfoResponse> txnGetLockInfo(StoreCtx storeCtx, Txn.GetLockInfoRequest.Builder request) {
    RequestContext context = new RequestContext(storeCtx, request.getKey(), request, ReqCase.GET_LOCK_INFO);
    return this.shardSender.sendReq(context).map(response -> (Txn.GetLockInfoResponse) response);
  }

  public Flux<List<Txn.KeyValue>> txnSingleScan(StoreCtx storeCtx, Txn.ScanRequest.Builder request,
                                                ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, request, ReqCase.SCAN);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }
    return this.shardSender.sendReq(context).map(response -> {
      Txn.ScanResponse scanResponse = (Txn.ScanResponse) response;
      if (scanResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "singleScan",
                String.valueOf(scanResponse.getCode()));
      }
      return scanResponse.getKvsList();
    });
  }

  //Txn.SelectFlowRequest.Builder
  public Flux<List<Txn.Row>> txnSelectFlowStream(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder, KvPair kvPair) {
    RoutePolicy routePolicy = storeCtx.getRoutePolicy();
    ByteString start = kvPair.getKey();
    byte[] key = NettyByteString.asByteArray(start);
    RangeInfo rangeInfo = routePolicy.getRangeInfoByKey(key);
    if (rangeInfo == null || StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
      LOG.error("locate route no exist by key {} for select, retry.", Arrays.toString(key));
      throw RangeRouteException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_NOT_EXIST, key);
    }
    return txnSingeSelectFlowStream(storeCtx, reqBuilder, NettyByteString.wrap(key), rangeInfo);
  }

  public Flux<List<Txn.KeyValue>> txnScan(StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder) {
    ByteString start = reqBuilder.getStartKey();
    ByteString end = reqBuilder.getEndKey();

    return txnRangeSelectFlux(storeCtx, scanRangeFunc, reqBuilder, start, end);
  }

  private <T> List<T> getRows(List<T> f1, List<T> f2) {
    if (f1.isEmpty()) {
      return f2;
    }
    if (f2.isEmpty()) {
      return f1;
    }
    List builders = new ArrayList<>(f1.size() + f2.size());
    builders.addAll(f1);
    builders.addAll(f2);
    return builders;
  }
  /* Top level callee by the analyze executor */

  /**
   * Send analyzeIndex request to the storage layer
   * @param storeCtx store context
   * @param reqBuilder request builder
   * @return flux of the response from the storage layer.
   */
  public Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(StoreCtx storeCtx, Statspb.IndexStatsRequest.Builder reqBuilder) {

    final ByteString startKey = reqBuilder.getRange().getStartKey();
    final ByteString endKey = reqBuilder.getRange().getEndKey();

    return txnRangeSelectFlux(storeCtx, singleAnalyzeIndexFunc, reqBuilder, startKey, endKey);
  }

  // singleAnalyzeIndexFunc
  private Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> singleAnalyzeIndex(StoreCtx storeCtx, Statspb.IndexStatsRequest.Builder reqBuilder, ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, ReqCase.INDEX_STATS);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }

    return this.shardSender.sendReq(context).map(r -> {
      Statspb.IndexStatsResponse response = (Statspb.IndexStatsResponse) r;
      if (response.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "analyzeIndex", String.valueOf(response.getCode()));
      }

      // note that this message may appear multiple times in the log because it is called within a loop
      LOG.debug("received response for analyzing index with columns {}: {}",
              reqBuilder.getColumnsInfoList().stream().map(Exprpb.ColumnInfo::getId).collect(Collectors.toList()), response);

      return Collections.singletonList(Tuples.of(response.getHist(), response.getCms()));
    });
  }

  /**
   * Send analyzeColumns request to the storage layer
   * @param storeCtx store context
   * @param reqBuilder request builder
   * @return flux of the response from the storage layer
   */
  public Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(StoreCtx storeCtx, Statspb.ColumnsStatsRequest.Builder reqBuilder) {
    final ByteString startKey = reqBuilder.getRange().getStartKey();
    final ByteString endKey = reqBuilder.getRange().getEndKey();

    return txnRangeSelectFlux(storeCtx, singleAnalyzeColumnsFunc, reqBuilder, startKey, endKey);
  }

  private Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> singleAnalyzeColumns(
          StoreCtx storeCtx, Statspb.ColumnsStatsRequest.Builder reqBuilder, ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, ReqCase.COLUMNS_STATS);
    if (rangeInfo == null) {
      context.refreshRangeInfo();
    }
    return this.shardSender.sendReq(context).map(r -> {
      Statspb.ColumnsStatsResponse response = (Statspb.ColumnsStatsResponse) r;
      if (response.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "analyzeColumns", String.valueOf(response.getCode()));
      }

      LOG.debug("received response for analyzing columns {}: {}",
              reqBuilder.getColumnsInfoList().stream().map(Exprpb.ColumnInfo::getId).collect(Collectors.toList()), response);

      return Collections.singletonList(Tuples.of(response.getPkHist(), response.getCollectorsList()));
    });
  }

  public void asyncTask(Runnable runnable) {
    try {
      this.asyncQueue.put(runnable);
    } catch (InterruptedException e) {
      LOG.warn("async queue interrupted");
    }
  }

  @Override
  public void close() {
    this.isRunning = false;
    this.shardSender.close();
    this.asyncQueue.clear();
    this.asyncExecutorPool.shutdown();
  }

  /**
   * @param <Resp>
   * @param <Req>
   * @version V1.0
   */
  @FunctionalInterface
  private interface SelectRangeFunc<Resp, Req> {
    Flux<Resp> apply(StoreCtx storeCtx, Req reqBuilder, ByteString key, RangeInfo rangeInfo);
  }
}
