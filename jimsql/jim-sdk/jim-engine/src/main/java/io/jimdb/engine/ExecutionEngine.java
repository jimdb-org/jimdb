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
package io.jimdb.engine;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.Value;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.engine.txn.TransactionImpl;
import io.jimdb.meta.Router;
import io.jimdb.meta.route.RoutingPolicy;
import io.jimdb.pb.Api;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Statspb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.NettyByteString;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * SQL execution engine
 */
public final class ExecutionEngine implements Engine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionEngine.class);

  // All the following members should be in singleton mode
  private Router router;

  private Dispatcher dispatcher;

  private ShardSender shardSender;

  private IdAllocator idAllocator;

  @Override
  public void init(JimConfig c) {
    idAllocator = new AutoIncrIdAllocator(PluginFactory.getMetaStore(), c.getRowIdStep());
    router = new Router(PluginFactory.getRouterStore(), c);

    // TODO we initialize the dispatcher here for now,
    //  in the future, we need to make it Singleton inside the Transaction since it's only been used there
    dispatcher = new AsyncTaskManager();

    shardSender = new ShardSender(c, router);
    try {
      dispatcher.start();
      shardSender.start();
    } catch (Exception e) {
      LOGGER.error("dispatcher start rpc error:{}", e.getMessage());
    }
  }

  @Override
  public Transaction beginTxn(Session session) {
    return new TransactionImpl(session, router, dispatcher, shardSender);
  }

  @Override
  public Set<RangeInfo> getRanges(Table table) throws BaseException {
    final int tableId = table.getId();
    final int dbId = table.getCatalog().getId();
    KvPair tableScope = Codec.encodeTableScope(tableId);
    RoutingPolicy routingPolicy = router.getOrCreatePolicy(dbId, tableId);

    byte[] startKey = NettyByteString.asByteArray(tableScope.getKey());
    byte[] endKey = NettyByteString.asByteArray(tableScope.getValue());
    return routingPolicy.getSerialRoute(startKey, endKey);
  }

  @Override
  public Flux<ExecResult> insert(Session session, Table table, Column[] insertCols, Expression[][] rows, Assignment[] duplicate, boolean hasRefColumn) throws BaseException {
    return InsertHandler.insert(idAllocator, session, table, insertCols, rows, duplicate, hasRefColumn);
  }

  @Override
  public Flux<ExecResult> update(Session session, Table table, Assignment[] assignments, QueryResult rows) throws BaseException {
    return UpdateHandler.update(shardSender, router, session, table, assignments, rows);
  }

  @Override
  public Flux<ExecResult> delete(Session session, Table table, QueryResult rows) throws BaseException {
    return DeleteHandler.delete(shardSender, router, session, table, rows);
  }

  @Override
  public Flux<ExecResult> get(Session session, List<Index> indices, List<Value[]> values, ColumnExpr[] resultColumns) {
    return SelectHandler.get(shardSender, router, session, indices, values, resultColumns);
  }

  @Override
  public byte[] addIndex(Session session, Index index, byte[] startKey, byte[] endKey, int limit) {
    return DdlHandler.addIndex(shardSender, router, session, index, startKey, endKey, limit);
  }

  @Override
  public Flux<ExecResult> select(Session session, Table table, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns, List<Integer> outputOffsetList) throws BaseException {
    return SelectHandler.select(shardSender, router, session, table, processors, resultColumns, outputOffsetList);
  }

  @Override
  public Flux<ExecResult> select(Session session, Index index, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns, List<Integer> outputOffsetList, List<ValueRange> ranges) throws BaseException {
    return SelectHandler.select(shardSender, router, session, index, processors, resultColumns, outputOffsetList, ranges);
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(Session session,
      Table table, Index index, Instant timeout, List<ValueRange> ranges, Statspb.IndexStatsRequest.Builder reqBuilder) {

    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges, false);
    Kv.KeyRange.Builder keyRangeBuilder = Kv.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sending request for analyzing index {} that has columns {}",
          index.getId(), Arrays.stream(index.getColumns()).map(Column::getId).collect(Collectors.toList()));
    }

    return AnalyzeHandler.analyzeIndex(shardSender, StoreCtx.buildCtx(session, table, router), reqBuilder);
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(Session session,
      Table table, Column[] columns, Instant timeout, List<ValueRange> ranges, Statspb.ColumnsStatsRequest.Builder reqBuilder) {
    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(table.getReadableIndices()[0], ranges, false); // table.getIndexes()[0] is the primary key index
    Kv.KeyRange.Builder keyRangeBuilder = Kv.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sending request for analyzing columns {}", Arrays.stream(columns).map(Column::getId).collect(Collectors.toList()));
    }

    return AnalyzeHandler.analyzeColumns(shardSender, StoreCtx.buildCtx(session, table, router), reqBuilder);
  }

  @Override
  public void close() {
    dispatcher.close();
    shardSender.close();
  }

  @Override
  @Deprecated
  public Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws BaseException {
    KvPair kvPair = new KvPair(NettyByteString.wrap(key), NettyByteString.wrap(value));
    StoreCtx storeCtx = StoreCtx.buildCtx(table, timeout, router);

    Message.Builder rawPutBuilder = Kv.KvPutRequest.newBuilder().setKey(kvPair.getKey()).setValue(kvPair.getValue());

    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(kvPair.getKey().toByteArray());
    RequestContext context = new RequestContext(storeCtx, kvPair.getKey(), rangeInfo, rawPutBuilder, Api.RangeRequest.ReqCase.KV_PUT);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvPutResponse resp = (Kv.KvPutResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawPut",
            String.valueOf(resp.getCode()));
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("raw put response success: {}", resp);
      }
      return Boolean.TRUE;
    });

  }

  @Override
  @Deprecated
  public Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws BaseException {
    StoreCtx storeCtx = StoreCtx.buildCtx(table, timeout, router);
    ByteString bytes = NettyByteString.wrap(key);
    Message.Builder builder = Kv.KvGetRequest.newBuilder().setKey(bytes);
    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(bytes.toByteArray());
    RequestContext context = new RequestContext(storeCtx, bytes, rangeInfo, builder, Api.RangeRequest.ReqCase.KV_GET);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvGetResponse resp = (Kv.KvGetResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawGet",
            String.valueOf(resp.getCode()));
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("raw get response success: {}", resp);
      }
      return NettyByteString.asByteArray(resp.getValue());
    });
  }

  @Override
  @Deprecated
  public Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws BaseException {
    StoreCtx storeCtx = StoreCtx.buildCtx(table, timeout, router);
    ByteString bytes = NettyByteString.wrap(key);
    Message.Builder builder = Kv.KvDeleteRequest.newBuilder().setKey(bytes);
    RangeInfo rangeInfo =  storeCtx.getRoutingPolicy().getRangeInfoByKey(bytes.toByteArray());
    RequestContext context = new RequestContext(storeCtx, bytes, rangeInfo, builder, Api.RangeRequest.ReqCase.KV_DELETE);
    return shardSender.sendReq(context).map(response -> {
      Kv.KvDeleteResponse resp = (Kv.KvDeleteResponse) response;
      if (resp.getCode() != 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "rawDelete",
            String.valueOf(resp.getCode()));
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("raw del response success: {}", resp);
      }
      return Boolean.TRUE;
    });
  }

}
