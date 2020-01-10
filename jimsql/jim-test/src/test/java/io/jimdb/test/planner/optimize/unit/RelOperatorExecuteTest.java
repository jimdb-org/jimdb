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
package io.jimdb.test.planner.optimize.unit;
//
///**
// * @version V1.0
// */
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingDeque;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//import io.jimdb.Bootstraps;
//import io.jimdb.Session;
//import io.jimdb.config.JimConfig;
//import io.jimdb.exception.JimException;
//import io.jimdb.expression.ColumnExpr;
//import io.jimdb.model.result.ExecResult;
//import io.jimdb.model.result.impl.DMLExecResult;
//import io.jimdb.model.result.impl.QueryExecResult;
//import io.jimdb.plugin.SQLEngine;
//import io.jimdb.plugin.store.Engine;
//import io.jimdb.plugin.store.Transaction;
//import io.jimdb.sql.JimSQLExecutor;
//import io.jimdb.test.mock.store.MockStoreEngine;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Hooks;
//
///**
// * @version V1.0
// */
//public class RelOperatorExecuteTest {
//  private String catalog;
//  private JimSQLExecutor sqlExecutor;
//
//  private static MockStoreEngine storeEngine;
//
//  private Session session;
//  private CountDownLatch latch;
//  private static ThreadPoolExecutor executor;
//  //maggie2: (h, id, v), primary key (`h`)
//  //maggie3: (h, id, v), primary key (`h`),  unique index (`id`)
//
//  @Before
//  public void before() throws Exception {
//    catalog = "test";
//    JimConfig config = Bootstraps.init("jim.properties");
//    storeEngine = new MockStoreEngine();
//    storeEngine.init(config);
//    sqlExecutor = (JimSQLExecutor) config.getSQLExecutor();
//    session = new MockSession(config.getSQLEngine(), storeEngine);
//    session.getVarContext().setDefaultCatalog(catalog);
//    session.getVarContext().setAutocommit(false);
//    Hooks.onOperatorDebug();
//  }
//
//  @After
//  public void after() throws Exception {
//  }
//
//  @Test
//  public void testTypeOfSupport() throws Exception {
//    checkTypeOfSupportForSelect();
//  }
//
//  @Test
//  public void testInsert() throws Exception {
//    String insertSql = "insert into test(h,id,v) VALUES (1,'id1','v2')";
//    executorSql(insertSql, 1);
//
//    Thread.sleep(10000);
//  }
//
//  @Test
//  public void testInsert1() throws Exception {
//    for (int i = 11; i < 30; i++) {
//      String insertSql = "insert into maggie2(h,id,v) VALUES (" + i + "," + (10 + i) + "," + (40 + i) + ")";
//      executorSql(insertSql, 1);
//
//      Thread.sleep(1000);
//    }
//  }
//
//  @Test
//  public void testInsertWithIncr() throws Exception {
//    String insertSql = "insert into maggie4(id,v) VALUES ('5','v2')";
//    executorSql(insertSql, 1);
//
//    Thread.sleep(10000);
//  }
//
//  @Test
//  public void testUpdate() throws Exception {
//    String updateSql = "update maggie2 set id = 'maggie3' where h = 1";
//    executorSql(updateSql, 1);
//  }
//
//  @Test
//  public void testUpdateAll() throws Exception {
//    //no support
//  }
//
//  @Test
//  public void testDelete() throws Exception {
//    String deleteSql = "delete from maggie2";
//    executorSql(deleteSql, 1);
//  }
//
//  @Test
//  public void testDeleteAll() throws Exception {
//    //no support
//  }
//
//  /**
//   * Method: parse(final String sql)
//   */
//  @Test
//  public void testParse() throws Exception {
////    final String selectSql = "select id AS field1, v AS field2 from maggie2 where h = 1 OR h = 2 or h = 3 or h = 4";
//    final String selectSql = "select id AS field1, v AS field2 from maggie2 where h = 1 and id = 'maggie3'";
//    executorSql(selectSql, 1);
//  }
//
//  @Test
//  public void testParsePerform() throws Exception {
//    int num = 1;
//    final String sql = "select id AS field1, v AS field2 from maggie2 where h = 1";
//    executor = new ThreadPoolExecutor(8,
//            8, 1, TimeUnit.MINUTES,
//            new LinkedBlockingDeque<>(num));
//    this.latch = new CountDownLatch(num);
//    long startTime = System.currentTimeMillis();
//    for (int i = 0; i < num; i++) {
//      executor.execute(() -> {
//        try {
//          sqlExecutor.executeQuery(session, sql);
//        } catch (RuntimeException e) {
//          System.out.println("execute sql " + sql + " error" + e.getMessage());
//        }
//      });
//    }
//    try {
//      latch.await();
//      long elapsedTime = System.currentTimeMillis() - startTime;
//      System.out.println("execute " + num + " elapse time:" + elapsedTime);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    } finally {
//      executor.shutdown();
//    }
//  }
//
//  private void executorSql(String executeSql, int num) {
//    this.latch = new CountDownLatch(num);
//    long start = System.currentTimeMillis();
//    for (int i = 0; i < num; i++) {
//      long startTime = System.currentTimeMillis();
//      try {
//        sqlExecutor.executeQuery(session, executeSql);
//      } catch (Throwable e) {
//        e.printStackTrace();
//        System.out.println("executor err " + e.getMessage());
//        return;
//      }
//      long elapsedTime = System.currentTimeMillis() - startTime;
//      System.out.println(i + "execute single time:" + elapsedTime);
//    }
//    try {
//      latch.await();
//      long elapsed = System.currentTimeMillis() - start;
//      System.out.println("execute " + num + " elapse time:" + elapsed);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//  }
//
//  private void checkTypeOfSupportForSelect() throws Exception {
//    String[] selectSqls = new String[]{
//            "show collection",
////            "select * from maggie2 order by id desc limit 1",
////            "select * from maggie2 limit 1",
////            "select  * from maggie2",
////            "select * from maggie2 where 1=1",
////            "select v from maggie2 where h= 1",
////            "select v from maggie2 where id='id1'",
////            "select id, v from maggie2 where id='id1'",
////            "select v from maggie2 where h = 1 or h = 2 or h = 3",
////            "select id, v AS field from maggie2 where h = 1",
////            "select id AS field1, v AS field2 from maggie2 where h = 1",
////            "select id field1, v field2 from maggie2 where h = 1",
////            "select * from maggie2 AS t",
////            "select * from maggie2 where h >= 1 and h < 5",
////            "select * from maggie2 where h < 1 and h > 5",
////            "select * from maggie2 where id > 'id2' and id < 'id5'",
////            "select * from maggie2 where h > 1 and id > 'id2'",
////            "select * from maggie2 limit 1",
////            "select * from maggie2 limit 1,1",
////            "select * from maggie2 where v ='b'",
////            "select count(*) from maggie2",
//
//            //no support
////            "select * from maggie2 limit 1, -1",
////            "select * from maggie2 where h between 1 and 5",
////            "select * from maggie2 where h in (1,2,3,4,5)",
////            "select distinct v from maggie2",
////            "select * from maggie2 order by id",
////            "select * from maggie2 order by id, h",
////            "select * from maggie2 group by h",
////            "select id, h from maggie2 group by id, h",
////            "select h, count(*) as total from maggie2 group by id having total > 1",
//    };
//    latch = new CountDownLatch(selectSqls.length);
//    for (String sql : selectSqls) {
//      System.out.println("start to execute:" + sql);
//      sqlExecutor.executeQuery(session, sql);
//    }
//    try {
//      latch.await();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   * mock sql session
//   */
//  private class MockSession extends Session {
//    MockSession(final SQLEngine sqlEngine, final Engine storeEngine) {
//      super(sqlEngine, storeEngine);
//    }
//
//    @Override
//    public void write(ExecResult rs, boolean needFlush) throws JimException {
//      if (rs != null) {
//        if (rs instanceof DMLExecResult) {
//          System.out.println("dml result" + rs.getAffectedRows());
//          //select no need commit
//          Transaction txn = getTxn();
//          Flux<ExecResult> flux = txn.commit();
//          flux.subscribe(m -> {
//            System.out.println("txn commit result ok");
//            latch.countDown();
//          }, e -> {
//            System.out.println("error" + e.getMessage());
//            latch.countDown();
//          });
//        } else if (rs instanceof QueryExecResult) {
//          latch.countDown();
//          if (rs.size() == 0) {
//            System.out.println("result size 0");
//          }
//
//          try {
//            ColumnExpr[] exprs = rs.getColumns();
//            Assert.assertEquals(1, rs.size());
//            rs.forEach(row -> {
//              for (int i = 0; i < exprs.length; i++) {
//                ColumnExpr column = exprs[i];
//                System.out.print("columns: " + column.getOriCol() + ", data: " + row.get(i, null).getString() + ", type:" + row.get(i, null).getType().toString() + ";  ");
//              }
//              System.out.println();
//            });
//          } catch (Exception e) {
//            e.printStackTrace();
//          }
//        }
//      }
//    }
//
//    @Override
//    public void writeError(JimException ex) throws JimException {
//      System.out.println("error:" + ex.getMessage());
//      latch.countDown();
//    }
//  }
//}
