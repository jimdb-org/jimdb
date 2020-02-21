///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.jimdb.engine.api;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import io.jimdb.api.JimTransactionManager;
//import io.jimdb.expression.Converter;
//import io.jimdb.model.Column;
//import io.jimdb.model.result.ExecResult;
//import io.jimdb.model.result.QueryExecResult;
//import io.jimdb.model.result.ResultColumn;
//import io.jimdb.pb.Exprpb;
//
//import org.apache.commons.lang3.ArrayUtils;
//import org.junit.Assert;
//import org.junit.Test;
//
//import reactor.core.publisher.Flux;
//
///**
// * @version V1.0
// */
//public class JimTransactionManagerTest extends JimClusterTest {
//  String tableName = "bnttest";
//  String[] cols = {"id","name","age"};
//
//  @Test
//  public void doAutoCommitTest() {
//
//    try {
//      //init data
//      Object[] ids = { 21, 22, 23 };
//      List<Object[]> rows = new ArrayList<>();
//      rows.add(new Object[]{ ids[0], "mike", 21 });
//      rows.add(new Object[]{ ids[1], "jerry", 30 });
//      rows.add(new Object[]{ ids[2], "tom", 40 });
//
//
//      JimTransactionManager txnm = jimDatabase.getTxManager();
//
//      //clear data from table
//      txnm.delete(tableName, ids);
//
//      long startTime = System.currentTimeMillis();
//      txnm = jimDatabase.getTxManager();
//
//      //1.insert
//      int result = txnm.insert(tableName, cols, rows);
//      Assert.assertEquals(rows.size(), result);
//
//      //2.select
//      List<Map<String, Object>> recordList = selectByIds(txnm, ids);
//      Assert.assertEquals(ids.length, recordList.size());
//
//      //3.update
//      int age = 100;
//      Map<String, Object> targetParams = new HashMap();
//      targetParams.put("age", age);
//      result = txnm.update(tableName, targetParams, ids);
//      Assert.assertEquals(ids.length, result);
//
//      //4.select
//      recordList = selectByIds(txnm, ids);
//      for (Map<String, Object> record : recordList) {
//        Assert.assertEquals(age, Integer.parseInt(record.get("age").toString()));
//      }
//      Assert.assertEquals(ids.length, recordList.size());
//
//      //5.clear data from table
//      result = txnm.delete(tableName, ids);
//      Assert.assertEquals(ids.length, result);
//
//      System.out.println("doAutoCommitTest succ!    elapse time(ms):" + (System.currentTimeMillis() - startTime));
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  private List<Map<String, Object>> selectByIds(JimTransactionManager txnm, Object[] ids) {
//    List<Map<String, Object>> recordList = new ArrayList<>();
//    try {
//      ExecResult result = txnm.select(tableName, cols, ids);
//
//      for (QueryExecResult.Row row : result.getRows()) {
//        Map<String, Object> rowMap = new HashMap<>();
//        for (int i = 0; i < result.getColumns().length; i++) {
//          rowMap.put(result.getColumns()[i].getOriColumn().getName(), row.getValue()[i]);
//        }
//        recordList.add(rowMap);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    return recordList;
//  }
//
//  private Map<String, Object> parseRow(ResultColumn[] cols, Object[] data) {
//    Map<String, Object> row = new HashMap<>();
//    for (int i = 0; i < cols.length; i++) {
//      row.put(cols[i].getOriColumn().getName(), data[i]);
//    }
//    return row;
//  }
//
//  @Test
//  public void useTxTest() {
//    try {
//      //init data
//      Object[] ids = { 121, 122, 123 };
//      List<Object[]> rows = new ArrayList<>();
//      rows.add(new Object[]{ ids[0], "mike", 31 });
//      rows.add(new Object[]{ ids[1], "jerry", 45 });
//      rows.add(new Object[]{ ids[2], "tom", 52 });
//
//      Object[] ids1 = {200, 201};
//      List<Object[]> rows1 = new ArrayList<>();
//      rows1.add(new Object[]{ ids1[0], "hankson", 70 });
//      rows1.add(new Object[]{ ids1[1], "forest", 81 });
//
//      Object[] allIds = ArrayUtils.addAll(ids, ids1);
//
//      JimTransactionManager txnm = jimDatabase.getTxManager();
//
//      //clear data  (auto commit)
//      txnm.delete(tableName, allIds);
//      System.out.println("clear data over");
//
//      //prepare data   (auto commit)
//      txnm = jimDatabase.getTxManager();
//      txnm.insert(tableName, cols, rows);
//      System.out.println("prepare data over");
//
//      //------------ start transaction (autoCommit = false)---------------//
//      txnm = jimDatabase.getTxManager();
//      txnm.setAutoCommit(false);
//      System.out.println("    tx start.... ");
//      long startTime = System.currentTimeMillis();
//
//      //1.insert
//      int result = txnm.insert(tableName, cols, rows1);
//      Assert.assertEquals(rows1.size(), result);
//
//      // check
//      Assert.assertEquals(0, selectByIds(txnm, ids1).size());
//
//      //2.update
//      int age = 100;
//      Map<String, Object> targetParams = new HashMap();
//      targetParams.put("age", age);
//      result = txnm.update(tableName, targetParams, ids);
//      Assert.assertEquals(ids.length, result);
//      txnm.commit();
//      System.out.println("    tx commit complete....     elapse time(ms):" + (System.currentTimeMillis() - startTime));
//      //------------ end transaction ---------------//
//
//      //check  ids
//      List<Map<String, Object>> recordList = selectByIds(txnm, ids);
//      for (Map<String, Object> record : recordList) {
//        Assert.assertEquals(age, Integer.parseInt(record.get("age").toString()));
//      }
//      Assert.assertEquals(ids.length, recordList.size());
//
//      //check  ids  and ids1
//      recordList = selectByIds(txnm, allIds);
//      Assert.assertEquals(ids.length + ids1.length, recordList.size());
//
//      System.out.println("useTxTest succ!");
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void doScanAndGetTest() {
//    try {
//      Object[] ids = { 201, 202, 203, 204, 205, 206, 207, 208 };
//      List<Object[]> rows = new ArrayList<>();
//      rows.add(new Object[]{ ids[0], "mike", 11 });
//      rows.add(new Object[]{ ids[1], "jerry", 11 });
//      rows.add(new Object[]{ ids[2], "tom", 52 });
//      rows.add(new Object[]{ ids[3], "tuk", 11 });
//      rows.add(new Object[]{ ids[4], "tank", 11 });
//      rows.add(new Object[]{ ids[5], "mock", 11 });
//      rows.add(new Object[]{ ids[6], "eric", 11 });
//      rows.add(new Object[]{ ids[7], "alex", 11 });
//
//      JimTransactionManager txnm = jimDatabase.getTxManager();
//
//      //clear data  (auto commit)
//      txnm.delete(tableName, ids);
//      System.out.println("clear data over");
//
//      //prepare data   (auto commit)
//      txnm = jimDatabase.getTxManager();
//      txnm.insert(tableName, cols, rows);
//
//      //select data
//      Exprpb.Expr expr = getExpr(txnm.getColumn(tableName, cols[2]), 11);
//      int pageSize = 5;
//      int pageCount1 = txnm.select(tableName, cols, expr, 0, pageSize).getRows().length;
//      int pageCount2 = txnm.select(tableName, cols, expr, 1, pageSize).getRows().length;
//      Assert.assertEquals(rows.size() - 1, pageCount1 + pageCount2);
//
//      //get data
//      ExecResult result = txnm.get(tableName, cols, ids[0]);
//      Object[] data = result.getRows()[0].getValue();
//      Map<String, Object> row = parseRow(result.getColumns(), data);
//      Assert.assertEquals(ids[0], row.get(cols[0]));
//
//      txnm.delete(tableName, ids);
//      System.out.println("clear data over");
//    } catch (Exception e) {
//      e.printStackTrace();
//      Assert.fail(e.getMessage());
//    }
//  }
//
//  private Exprpb.Expr getExpr(Column column, int condition) {
//    List<Exprpb.Expr> exprList = new ArrayList<>();
//    exprList.add(Exprpb.Expr.newBuilder()
//            .setExprType(Exprpb.ExprType.Column)
//            .setColumn(Converter.convertColumn(column)).build());
//    exprList.add(Exprpb.Expr.newBuilder()
//            .setExprType(Exprpb.ExprType.Const_Int)
//            .setValue(Converter.encodeValue(condition)).build());
//
//    return Exprpb.Expr.newBuilder()
//            .setExprType(Exprpb.ExprType.Equal)
//            .addAllChild(exprList)
//            .build();
//  }
//
//
//  @Test
//  public void doTest() {
//    Flux.just("a", "b")
//            .zipWith(Flux.just("c", "d"))
//            .zipWith(Flux.just("e", "f"))
//            .subscribe(System.out::println);
//  }
//}
