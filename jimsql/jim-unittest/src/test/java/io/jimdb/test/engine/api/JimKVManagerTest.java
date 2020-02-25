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
//import java.util.List;
//import java.util.Map;
//
//import io.jimdb.api.JimKVManager;
//
//import org.junit.Assert;
//import org.junit.Test;
//
///**
// * @version V1.0
// */
//public class JimKVManagerTest extends JimClusterTest {
//  String tableName = "bntkvtest";
//  String[] cols = { "id", "tkey", "tvalue" };
//
//  @Test
//  public void doPutListTest() {
//    try {
//      //init data
//      Object[] ids = { 1, 2, 3 };
//      List<Object[]> rows = new ArrayList<>();
//      rows.add(new Object[]{ ids[0], "interval", "3000" });
//      rows.add(new Object[]{ ids[1], "timeunit", "second" });
//      rows.add(new Object[]{ ids[2], "timeout", "1000" });
//
//      JimKVManager kvManager = jimDatabase.getKVManager();
//
//      kvManager.put(tableName, cols, rows);
//      Thread.sleep(3000);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void doOPTest() {
//    try {
//      //init data
//      Object[] pk = { 6 };
//      Object[] row = { pk[0], "io", "6" };
//
//      JimKVManager kvManager = jimDatabase.getKVManager();
//
//      //put
//      kvManager.put(tableName, cols, row);
//
//      //get
//      JimKVManager.Record record = kvManager.get(tableName, pk);
//      for (Map.Entry<String, Object> kv : record.kvMap.entrySet()) {
//        System.out.println("key:" + kv.getKey() + ",  value: " + new String((byte[])kv.getValue()));
//      }
//
//      //delete
//      kvManager.delete(tableName, pk);
//
//      //get
//      record = kvManager.get(tableName, pk);
//      Assert.assertNull(record);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void doDeleteTest() {
//    try {
//      //init data
//      Object[] pk = { 6 };
//
//      JimKVManager kvManager = jimDatabase.getKVManager();
//
//      long startTime = System.currentTimeMillis();
//      kvManager.delete(tableName, pk);
//      System.out.println("time: " + (System.currentTimeMillis() - startTime));
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//}
