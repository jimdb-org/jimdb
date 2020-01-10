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
package io.jimdb.jmh;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.jimdb.core.Bootstraps;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.engine.JimStoreEngine;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Basepb;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 * <p>
 * engine test
 */
//@BenchmarkMode(Mode.AverageTime) // 测试方法平均执行时间
@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
@OutputTimeUnit(TimeUnit.SECONDS)// 输出结果的时间粒度为秒
@State(Scope.Benchmark) // 每个测试线程一个实例
public class EngineBenchMark {
  private static final Logger LOG = LoggerFactory.getLogger(EngineBenchMark.class);
  private static final AtomicLong ID = new AtomicLong(0L);
  JimStoreEngine engine;
  int type;
  int dataLength;
  int dataBatch;
  String catalog;
  String tableName;
  Table t;
  List<Index> pkIndexes;
  List<Index> uniqueIndexes;
  ColumnExpr[] exprs;
  ColumnExpr[] exprsNoSystem;
  private Session session;

  public EngineBenchMark() {
  }

  @Setup(Level.Trial)
  public void init() {
    try {
      JimConfig config = Bootstraps.init("jim.properties");
      this.session = new Session(PluginFactory.getSqlEngine(), PluginFactory.getStoreEngine());
      this.catalog = config.getString("catalog", "maggie");
      this.tableName = config.getString("table.name", "maggie1");
      this.type = config.getInt("test.type", 1);
      this.dataLength = config.getInt("test.data.len", 256);
      this.dataBatch = config.getInt("test.data.batch", 1);
      Engine e = PluginFactory.getStoreEngine();
      if (!(e instanceof JimStoreEngine)) {
        throw new IllegalArgumentException("engine error");
      }
      this.engine = (JimStoreEngine) e;
      this.t = MetaData.Holder.getMetaData().getTable(this.catalog, this.tableName);

      Index pkIndex = null, uniqueIndex = null;
      for (Index index : t.getWritableIndices()) {
        if (index.isUnique()) {
          for (Column column : index.getColumns()) {
            if (column.getName().equals("h")) {
              pkIndex = index;
              continue;
            }
            if (column.getName().equals("id")) {
              uniqueIndex = index;
              continue;
            }
          }
        }
      }

      pkIndexes = new ArrayList<>();
      pkIndexes.add(pkIndex);
      uniqueIndexes = new ArrayList<>();
      uniqueIndexes.add(uniqueIndex);

      List<ColumnExpr> columnExprs = new ArrayList<>();
      List<ColumnExpr> columnExprsNoSys = new ArrayList<>();
      for (Column column : t.getWritableColumns()) {
        ColumnExpr expr = buildColumn(t, column, t.getName(), column.getName());
        columnExprsNoSys.add(expr);
        columnExprs.add(expr);
      }
      exprs = columnExprs.toArray(new ColumnExpr[columnExprs.size()]);
      exprsNoSystem = columnExprsNoSys.toArray(new ColumnExpr[columnExprsNoSys.size()]);
    } catch (Throwable var2) {
      var2.printStackTrace();
    }
  }

  private static ColumnExpr buildColumn(Table table, Column column, String aliasTable, String aliasColumn) {
    ColumnExpr result = new ColumnExpr(column.getId().longValue(), column);
    result.setCatalog(table.getCatalog().getName());
    result.setOriTable(table.getName());
    result.setAliasTable(aliasTable);
    result.setAliasCol(aliasColumn);
    return result;
  }

  @Benchmark
  public void testMethod() {
    if (type == 1) {
      testInsert();
    } else {
      testSelectByPkIndex();
    }
//    else if (type == 2) {
//      testUpdateByPkIdx();
//    } else if (type == 3) {
//      testUpdateByUniqueIdx();
//    } else if (type == 4) {
//      testDeleteByPkIdx();
//    } else if (type == 5) {
//      testDeleteByUniqueIdx();
//    } else if (type == 6) {
//      testSelectByPk();
//    } else if (type == 7) {
//      testSelectByUnique();
//    } else if (type == 8) {
//      testSelectScan();
//    }
  }

  public void testInsert() {
    //meta change test
//    MetaEngine.MetaData metaData = meta.getMetaData();
//    Table t = metaData.getTable(catalog, tableName);
//    if (t == null) {
//      LOG.error("table is null");
//      return;
//    }

    //meta change test
    MetaData metaData = MetaData.Holder.getMetaData();
    Table t = metaData.getTable(catalog, tableName);
    if (t == null) {
      LOG.error("table is null");
      return;
    }

    List<Expression[]> exprList = new ArrayList<>();
    for (int i = 0; i < dataBatch; i++) {
      long rowId = ID.incrementAndGet();
      Expression[] exprs = new Expression[4];
      exprs[0] = new ValueExpr(NullValue.getInstance(), Types.buildSQLType(Basepb.DataType.BigInt));
      exprs[1] = new ValueExpr(LongValue.getInstance(rowId), Types.buildSQLType(Basepb.DataType.BigInt));
      exprs[2] = new ValueExpr(StringValue.getInstance(String.format("%d", rowId)), Types.buildSQLType(Basepb.DataType.Varchar));
      exprs[3] = new ValueExpr(StringValue.getInstance(TestUtil.getRandomString(this.dataLength)), Types.buildSQLType(Basepb.DataType.BigInt));
      exprList.add(exprs);
    }

    CountDownLatch latch = new CountDownLatch(1);
    Transaction txn = this.engine.beginTxn(this.session);

    try {
      Flux<ExecResult> flux = txn.insert(this.t, this.t.getWritableColumns(), exprList, null, false).flatMap((execResult) -> {
        if (LOG.isInfoEnabled()) {
          LOG.info("insert success, affected row" + execResult.getAffectedRows());
        }
        return txn.commit();
      });

      subscribe(flux, latch);

      try {
        latch.await();
      } catch (InterruptedException var7) {
        LOG.error("wait err ", var7);
      }
    } catch (Throwable var8) {
      LOG.error("throw  err", var8);
    }
  }

//  public void testUpdateByPkIdx() {
//    //update maggie3 set v = "maggie" where h = 1;
//
//    long rowId = ID.incrementAndGet();
//    Object[] idxVals = new Object[1];
//    idxVals[0] = rowId;
//
//    Object[] colVals = new Object[2];
//    colVals[0] = String.format("%d", rowId);//跟之前insert的值一样，所以并不更新索引值
//    colVals[1] = "maggie";
//
//    CountDownLatch latch = new CountDownLatch(1);
//    Transaction txn = this.engine.beginTxn();
//    try {
//      Flux<ExecResult> pkSelectFlux = txn.getByIndex(resultColumns, pkIndex, idxVals)
//              .flatMap(r -> {
//                QueryExecResult rs = (QueryExecResult) r;
//                QueryExecResult.Row[] rows = rs.getRows();
//                if (rows == null || rows.length == 0) {
//                  LOG.warn("no data: select by pk rowId {}.", rowId);
//                  return Flux.just(new DMLExecResult(0));
//                }
//                AssignmentV1[] assignments = TestUtil.buildUpdateList(t, colVals);
//                return txn.update(t, assignments, rows);
//              }).flatMap(r -> txn.commit());
//
//      subscribe(pkSelectFlux, latch);
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }

  //  public void testUpdateByUniqueIdx() {
//    //update maggie3 set v = "100" where id = "1";
//    long rowId = ID.incrementAndGet();
//    Object[] idxVals = new Object[1];
//    idxVals[0] = String.format("%d", rowId);
//
//    Object[] colVals = new Object[2];
//    colVals[0] = String.format("%d", rowId);//跟之前insert的值一样，所以并不更新索引值
//    colVals[1] = "maggie2";
//
//    CountDownLatch latch = new CountDownLatch(1);
//    Transaction txn = this.engine.beginTxn();
//    try {
//      Flux<ExecResult> uniqueIdxSelectFlux = txn.getByIndex(resultColumns, uniqueIndex, idxVals)
//              .flatMap(r -> {
//                QueryExecResult rs = (QueryExecResult) r;
//                QueryExecResult.Row[] rows = rs.getRows();
//                if (rows == null || rows.length == 0) {
//                  LOG.warn("no data: select by unique index {}.", rowId);
//                  return Flux.just(new DMLExecResult(0));
//                }
//
//                AssignmentV1[] assignments = TestUtil.buildUpdateList(t, colVals);
//                return txn.update(t, assignments, rows);
//              }).flatMap(r -> txn.commit());
//      ;
//
//      subscribe(uniqueIdxSelectFlux, latch);
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }
//
//  public void testDeleteByPkIdx() {
//    long rowId = ID.incrementAndGet();
//    Object[] row = new Object[]{ rowId };
//    CountDownLatch latch = new CountDownLatch(1);
//
//    Transaction txn = this.engine.beginTxn();
//    try {
//      Flux<ExecResult> flux = txn.getByIndex(resultColumns, pkIndex, row).flatMap(r -> {
//        QueryExecResult rs = (QueryExecResult) r;
//        QueryExecResult.Row[] rows = rs.getRows();
//        if (rows == null || rows.length == 0) {
//          LOG.warn("no data: delete by pk rowId {}.", rowId);
//          return Flux.just(new DMLExecResult(0));
//        }
//        return txn.delete(t, resultColumns, rows);
//      }).flatMap(r -> txn.commit());
//
//      subscribe(flux, latch);
//
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }
//
//  public void testDeleteByUniqueIdx() {
//    long rowId = ID.incrementAndGet();
//    String value = String.format("%d", rowId);
//    Object[] row = new Object[]{ value };
//    CountDownLatch latch = new CountDownLatch(1);
//
//    Transaction txn = this.engine.beginTxn();
//    try {
//      Flux<ExecResult> flux = txn.getByIndex(resultColumns, pkIndex, row).flatMap(r -> {
//        QueryExecResult rs = (QueryExecResult) r;
//        QueryExecResult.Row[] rows = rs.getRows();
//        if (rows == null || rows.length == 0) {
//          LOG.warn("no data: delete by unique index {}.", value);
//          return Flux.just(new DMLExecResult(0));
//        }
//        return txn.delete(t, resultColumns, rows);
//      }).flatMap(r -> txn.commit());
//
//      subscribe(flux, latch);
//
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }
//
  public void testSelectByPkIndex() {
    Value[] row = new Value[1];
    row[0] = LongValue.getInstance(ID.incrementAndGet());
    List<Value[]> values = new ArrayList<>();
    values.add(row);

    Transaction txn = this.engine.beginTxn(this.session);
    CountDownLatch latch = new CountDownLatch(1);
    try {
      Flux<ExecResult> flux = txn.get(pkIndexes, values, exprsNoSystem);
      subscribe(flux, latch);

      try {
        latch.await();
      } catch (InterruptedException var7) {
        LOG.error("wait err ", var7);
      }
    } catch (Throwable var8) {
      LOG.error("throw  err", var8);
    }
  }

//  public void testSelectByUnique() {
//    Object[] row = new Object[]{ String.format("%d", ID.incrementAndGet()) };
//
//    Transaction txn = this.engine.beginTxn();
//    CountDownLatch latch = new CountDownLatch(1);
//    try {
//      Flux<ExecResult> flux = txn.getByIndex(resultColumns, uniqueIndex, row);
//
//      subscribe(flux, latch);
//
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }
//
//  public void testSelectScan() {
//    long id = ID.incrementAndGet();
//
//    Exprpb.Expr leftChild = Exprpb.Expr.newBuilder().setExprType(Exprpb.ExprType.Column)
//            .setColumn(Converter.convertColumn(pkIndex.getColumns()[0])).build();
//    Exprpb.Expr rightChild = Exprpb.Expr.newBuilder().setExprType(Exprpb.ExprType.Const_Int)
//            .setValue(Converter.encodeValue(id)).build();
//    Exprpb.Expr expr = Exprpb.Expr.newBuilder()
//            .setExprType(Exprpb.ExprType.Equal)
//            .addChild(leftChild)
//            .addChild(rightChild).build();
//
//    Transaction txn = this.engine.beginTxn();
//    CountDownLatch latch = new CountDownLatch(1);
//    try {
//      Flux<ExecResult> flux = txn.scan(t, resultColumns, expr, null, null);
//
//      subscribe(flux, latch);
//
//      try {
//        latch.await();
//      } catch (InterruptedException var7) {
//        LOG.error("wait err ", var7);
//      }
//    } catch (Throwable var8) {
//      LOG.error("throw  err", var8);
//    }
//  }
//
//  public void testRawPut() {
//    long rowId = ID.incrementAndGet();
//    Object[] row = new Object[]{ rowId, String.format("%d", rowId), null };
//    row[2] = row[1];
//    CountDownLatch latch = new CountDownLatch(1);
//    try {
//      Flux<Kv.KvPutResponse> flux = engine.rawPut(t, t.getColumns(), row);
//      subscribe(flux, latch);
//      try {
//        latch.await();
//      } catch (InterruptedException e) {
//        LOG.error("wait err ", e);
//      }
//    } catch (Throwable e2) {
//      LOG.error("throw  err ", e2);
//      e2.printStackTrace();
//    }
//  }
//
//  public void testRawGet() {
//    Object[] row = new Object[]{ ID.incrementAndGet() };
//    CountDownLatch latch = new CountDownLatch(1);
//    try {
//      Flux<Kv.KvGetResponse> flux = engine.rawGet(t, t.getColumns(), row);
//      subscribe(flux, latch);
//      try {
//        latch.await();
//      } catch (InterruptedException e) {
//        LOG.error("wait err ", e);
//      }
//    } catch (Throwable e2) {
//      LOG.error("throw  err ", e2);
//    }
//  }

  private <T> void subscribe(Flux<T> flux, CountDownLatch latch) {
    flux.subscribe(new BaseSubscriber() {
      @Override
      protected void hookOnSubscribe(Subscription subscription) {
        request(1);
      }

      @Override
      protected void hookOnNext(Object value) {
        latch.countDown();
      }

      @Override
      protected void hookOnError(Throwable e) {
        LOG.error("response err:  ", e);
        latch.countDown();
      }
    });
  }

  @TearDown
  public void after() {
  }

  public static void main(String[] args) throws RunnerException {
    int threadNum = 20;
    if (args.length > 0) {
      threadNum = Integer.parseInt(args[0]);
    }
    System.out.println(threadNum);
    Options opt = new OptionsBuilder().include(EngineBenchMark.class.getSimpleName())
            .forks(1)
            .threads(threadNum)
            .warmupIterations(5)
            .measurementIterations(5000000)
            .build();
    new Runner(opt).run();
  }
}
