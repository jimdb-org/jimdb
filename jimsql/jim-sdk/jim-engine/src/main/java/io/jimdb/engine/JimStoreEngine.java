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
package io.jimdb.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.context.ReorgContext;
import io.jimdb.engine.sender.DistSender;
import io.jimdb.engine.table.alloc.AutoIncrIDAllocator;
import io.jimdb.engine.table.alloc.IDAllocator;
import io.jimdb.engine.txn.JimTransaction;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.meta.RouterManager;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Statspb;
import io.jimdb.pb.Txn;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.common.utils.lang.ByteUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * storage engine
 *
 * @version V1.0
 */
public class JimStoreEngine implements Engine {
  private static final Logger LOGGER = LoggerFactory.getLogger(JimStoreEngine.class);

  private int concurrentNum = ReorgContext.REORG_CONCURRENT_NUM;
  private ThreadPoolExecutor reorgExecutorPool = new ThreadPoolExecutor(concurrentNum, concurrentNum,
          1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());

  //route
  private RouterManager routerManager;

  private DistSender distSender;

  private IDAllocator idAllocator;

  @Override
  public void init(JimConfig c) {
    this.idAllocator = new AutoIncrIDAllocator(PluginFactory.getMetaStore(), c.getRowIdStep());
    this.routerManager = new RouterManager(PluginFactory.getRouterStore(), c);
    this.distSender = new DistSender(c, this.routerManager);
    try {
      this.distSender.start();
    } catch (Exception e) {
      LOGGER.error("distSender start rpc error:{}", e.getMessage());
    }
  }

  @Override
  public Transaction beginTxn(Session session) {
    return new JimTransaction(session, routerManager, distSender, idAllocator);
  }

  @Override
  public Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws JimException {
    KvPair keyValue = new KvPair(NettyByteString.wrap(key), NettyByteString.wrap(value));
    return this.distSender.rawPut(StoreCtx.buildCtx(table, timeout, routerManager, distSender), keyValue);
  }

  @Override
  public Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws JimException {
    return this.distSender.rawGet(StoreCtx.buildCtx(table, timeout, routerManager, distSender), key);
  }

  @Override
  public Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws JimException {
    return this.distSender.rawDel(StoreCtx.buildCtx(table, timeout, routerManager, distSender), key);
  }

  @Override
  public Set<RangeInfo> getRanges(Table table) throws JimException {
    final int tableId = table.getId();
    final int dbId = table.getCatalog().getId();
    KvPair tableScope = Codec.encodeTableScope(tableId);
    RoutePolicy routePolicy = this.routerManager.getOrCreatePolicy(dbId, tableId);

    byte[] startKey = NettyByteString.asByteArray(tableScope.getKey());
    byte[] endKey = NettyByteString.asByteArray(tableScope.getValue());
    return routePolicy.getSerialRoute(startKey, endKey);
  }

  @Override
  public void reOrganize(ReorgContext context, Table table, Index index, Ddlpb.OpType opType) throws JimException {
    switch (opType) {
      case AddIndex:
        reOrgAddIndex(context, table, index);
        return;
      default:
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_CHECK_NOT_IMPLEMENTED,
                String.format("reorg Type: %s", opType));
    }
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  private void reOrgAddIndex(ReorgContext context, Table table, Index index) {
    int tableId = table.getId();
    RoutePolicy routePolicy = this.routerManager.getOrCreatePolicy(table.getCatalog().getId(), tableId);
    KvPair tableScope = Codec.encodeTableScope(tableId);
    byte[] startKey = NettyByteString.asByteArray(tableScope.getKey());
    byte[] endKey = NettyByteString.asByteArray(tableScope.getValue());
    Set<RangeInfo> routeSet;
    try {
      routeSet = routePolicy.getSerialRoute(startKey, endKey);
    } catch (Throwable e) {
      LOGGER.error("serial route error:", e);
      throw e;
    }

    ColumnExpr[] columnExprs = getColumnExprs(table, index);
    List<Exprpb.ColumnInfo> columnInfos = getColumnInfos(columnExprs);
    List<Integer> outputOffsetList = getOffsetList(columnExprs);

    int routeSize = routeSet.size();
    RangeInfo[] routeArray = new RangeInfo[routeSize];
    routeSet.toArray(routeArray);

    int loopNum = routeSize / concurrentNum;
    if (routeSize % concurrentNum != 0) {
      loopNum += 1;
    }

    for (int i = 0; i < loopNum; i++) {
      CountDownLatch latch;
      if (i < loopNum - 1) {
        latch = new CountDownLatch(concurrentNum);
      } else {
        latch = new CountDownLatch(routeSize - i * concurrentNum);
      }
      for (int j = 0; j < concurrentNum; j++) {
        int scale = i * concurrentNum + j;
        if (scale >= routeSize) {
          break;
        }
        final RangeInfo range = routeArray[scale];
        reorgExecutorPool.execute(() -> {
          try {
            ByteString rStartKey = NettyByteString.wrap(range.getStartKey());
            ByteString rEndKey = NettyByteString.wrap(range.getEndKey());
            handleForAddIndex(context, index, columnExprs, columnInfos, outputOffsetList, rStartKey, rEndKey);
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info("reorg table{} index {} range:[{},{}] success", table.getName(), index.getName(),
                      Arrays.toString(rStartKey.toByteArray()), Arrays.toString(rEndKey.toByteArray()));
            }
            //give up CPU
            Thread.sleep(1000);
          } catch (Throwable e) {
            LOGGER.error("reorg by range failed, err:", e);
            context.setFailed(true, e);
            return;
          } finally {
            latch.countDown();
          }
        });
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        LOGGER.error(" reorg latch await err, ", e);
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_UNKNOWN_ERROR, e, e.getMessage());
      }
    }
  }

  private void handleForAddIndex(ReorgContext context, Index index, ColumnExpr[] columnExprs,
                                 List<Exprpb.ColumnInfo> columnInfos, List<Integer> offsetList,
                                 ByteString startKey, ByteString endKey) {
    Txn.SelectFlowRequest.Builder reqBuilder = getSelectFlowBuilder(columnInfos, offsetList);
    ByteString reqStartKey = startKey;
    ByteString reqEndKey = endKey;

    do {
      if (context.isFailed()) {
        LOGGER.warn("reorg context is failed, quit");
        return;
      }
      //table read, query row key by limit
      reqBuilder.getProcessorsBuilder(0).getTableReadBuilder()
              .setRange(Processorpb.KeyRange.newBuilder().setStartKey(reqStartKey).setEndKey(reqEndKey));

      Transaction txn = this.beginTxn(null);
      ValueAccessor[] valueAccessors = txn.selectByStream(index.getTable(), reqBuilder, columnExprs).blockFirst();
      if (valueAccessors != null && valueAccessors.length > 0) {
        //re-organization index data
        txn.addIndex(index, columnExprs, valueAccessors);
        try {
          txn.commit().blockFirst();
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("reorg [{}] subscribe success", Arrays.toString(NettyByteString.asByteArray(reqEndKey)));
          }
        } catch (Throwable err) {
          if (err instanceof JimException) {
            JimException exception = (JimException) err;
            if (exception.getCode() == ErrorCode.ER_TXN_CONFLICT || exception.getCode() == ErrorCode.ER_TXN_VERSION_CONFLICT
                    || exception.getCode() == ErrorCode.ER_TXN_STATUS_CONFLICT) {
              //retry
              continue;
            }
          }
          throw err;
        }
      }

      ByteString reqLastKey = reqBuilder.getProcessors(0).getTableRead().getRange().getEndKey();
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("select stream <add index>: response startKey:{}, endKey:{}",
                Arrays.toString(NettyByteString.asByteArray(reqStartKey)),
                Arrays.toString(NettyByteString.asByteArray(reqLastKey)));
      }
      if (ByteUtil.compare(reqLastKey, reqEndKey) < 0) {
        reqStartKey = Codec.nextKey(reqLastKey);
        continue;
      }
      break;
    } while (true);
  }

  private ColumnExpr[] getColumnExprs(Table table, Index index) {

    Column[] pkColumns = table.getPrimary();
    Column[] idxColumns = index.getColumns();

    ColumnExpr[] exprs = new ColumnExpr[pkColumns.length + idxColumns.length];
    int i = 0;
    for (Column column : pkColumns) {
      exprs[i] = new ColumnExpr(Long.valueOf(i), column);
      i++;
    }
    for (Column column : idxColumns) {
      exprs[i] = new ColumnExpr(Long.valueOf(i), column);
      i++;
    }
    return exprs;
  }

  private List<Exprpb.ColumnInfo> getColumnInfos(ColumnExpr[] exprs) {
    List<Exprpb.ColumnInfo> columnInfos = new ArrayList<>(exprs.length);
    for (ColumnExpr expr : exprs) {
      Exprpb.ColumnInfo.Builder columnInfo = Exprpb.ColumnInfo.newBuilder()
              .setId(expr.getId().intValue())
              .setTyp(expr.getResultType().getType())
              .setUnsigned(expr.getResultType().getUnsigned());
      if (expr.getReorgValue() != null) {
        columnInfo.setReorgValue(expr.getReorgValue());
      }
      columnInfos.add(columnInfo.build());
    }
    return columnInfos;
  }

  private List<Integer> getOffsetList(ColumnExpr[] columnExprs) {
    List<Integer> outputOffsetList = new ArrayList<>(columnExprs.length);
    for (int i = 0; i < columnExprs.length; i++) {
      outputOffsetList.add(i);
    }
    return outputOffsetList;
  }

  //select limit REORG_TXN_LIMIT_NUM; return lastKey
  private Txn.SelectFlowRequest.Builder getSelectFlowBuilder(List<Exprpb.ColumnInfo> columnInfos,
                                                             List<Integer> outputOffsetList) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    Processorpb.Processor.Builder tableRead = Processorpb.Processor.newBuilder()
            .setType(Processorpb.ProcessorType.TABLE_READ_TYPE)
            .setTableRead(Processorpb.TableRead.newBuilder().setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                    .addAllColumns(columnInfos));
    builder.addProcessors(tableRead);
    Processorpb.Processor.Builder limit = Processorpb.Processor.newBuilder()
            .setType(Processorpb.ProcessorType.LIMIT_TYPE)
            .setLimit(Processorpb.Limit.newBuilder().setCount(ReorgContext.REORG_TXN_LIMIT_NUM));
    builder.addProcessors(limit);
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(false);
    return builder;
  }

  @Override
  public void close() {
    this.distSender.close();
    if (this.reorgExecutorPool != null) {
      this.reorgExecutorPool.shutdown();
    }
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(
          Table table, Index index, Instant timeout, List<ValueRange> ranges, Statspb.IndexStatsRequest.Builder reqBuilder) {

    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges);
    Processorpb.KeyRange.Builder keyRangeBuilder = Processorpb.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    LOGGER.debug("sending request for analyzing index {} that has columns {}",
            index.getId(), Arrays.stream(index.getColumns()).map(Column::getId).collect(Collectors.toList()));

    return this.distSender.analyzeIndex(StoreCtx.buildCtx(table, timeout, routerManager, distSender), reqBuilder);
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(
          Table table, Column[] columns, Instant timeout, List<ValueRange> ranges, Statspb.ColumnsStatsRequest.Builder reqBuilder) {
    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(table.getReadableIndices()[0], ranges); // table.getIndexes()[0] is the primary key index
    Processorpb.KeyRange.Builder keyRangeBuilder = Processorpb.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    LOGGER.debug("sending request for analyzing columns {}", Arrays.stream(columns).map(Column::getId).collect(Collectors.toList()));

    return this.distSender.analyzeColumns(StoreCtx.buildCtx(table, timeout, routerManager, distSender), reqBuilder);
  }
}
