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

package io.jimdb.test.planner.optimize.integration;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import io.jimdb.exception.JimException;
//import io.jimdb.expression.ColumnExpr;
//import io.jimdb.expression.RowValueAccessor;
//import io.jimdb.expression.ValueAccessor;
//import io.jimdb.test.mock.store.MockTableData;
//import io.jimdb.test.planner.TestBase;
//import io.jimdb.values.BinaryValue;
//import io.jimdb.values.LongValue;
//import io.jimdb.values.Value;
//
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//public class PhysicalExecuteTest extends TestBase {
//
//  private static List<MockTableData.User> userList = new ArrayList<>();
//
//  @Before
//  public void testLoadData(){
////    loadOrderData();
////    loadLimitData();
//    loadSelection();
////    loadIndexSourceData();
//  }
//
//  @Test
//  public void testExecSqlAllCases() {
//    List<Checker> checkerList = buildExecTestCases();
//    checkerList.forEach(checker -> {
//      try {
//        checkExecute(checker.getSql());
//        checker.doCheck(null);
//      } catch (JimException e) {
//        e.printStackTrace();
//        Assert.assertTrue(checker.isHasException());
//      }
//    });
//  }
//
//  private List<Checker> buildExecTestCases() {
//    List<Checker> checkerList = new ArrayList<>();
//
//    // limit
//    // tableSource
////    checkerList.add(Checker.build()
////            .sql("select name, age from user limit 2")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Tom', age=30, phone='null', score=0, salary=0}, "
////                    + "User{name='Jack', age=20, phone='null', score=0, salary=0}]"));
//
////    //order by
////    checkerList.add(Checker.build()
////            .sql("select name, age, phone, score, salary from user order by age, score desc")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Sophia', age=10, phone='13010016666', score=80, salary=4000}, "
////                    + "User{name='Jack', age=20, phone='13010011111', score=100, salary=6000}, "
////                    + "User{name='Luke', age=20, phone='13010015555', score=100, salary=5000}, "
////                    + "User{name='Tom', age=30, phone='13010010000', score=90, salary=5000}, "
////                    + "User{name='Kate', age=30, phone='13010014444', score=80, salary=9000}, "
////                    + "User{name='Bob', age=30, phone='13010018888', score=40, salary=2000}, "
////                    + "User{name='Suzy', age=40, phone='13010013333', score=90, salary=8000}, "
////                    + "User{name='Daniel', age=40, phone='13010019999', score=70, salary=5000}, "
////                    + "User{name='Mary', age=50, phone='13010012222', score=60, salary=7000}, "
////                    + "User{name='Steven', age=60, phone='13010017777', score=50, salary=3000}]"));
//
////    //topN
////    checkerList.add(Checker.build()
////            .sql("select name, age from user order by age desc limit 3")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Steven', age=60, phone='null', score=0, salary=0}, "
////                    + "User{name='Mary', age=50, phone='null', score=0, salary=0}, "
////                    + "User{name='Suzy', age=40, phone='null', score=0, salary=0}]"));
////
////    //KeyGet
////    checkerList.add(Checker.build()
////            .sql("select name, age, phone, score, salary from user where name = \"Tom\"")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Tom', age=30, phone='13010010000', score=90, salary=5000}]"));
////
////    //projection
////    checkerList.add(Checker.build()
////            .sql("select name,age from user")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Tom', age=30, phone='null', score=0, salary=0}, "
////                    + "User{name='Jack', age=20, phone='null', score=0, salary=0}, "
////                    + "User{name='Mary', age=50, phone='null', score=0, salary=0}, "
////                    + "User{name='Suzy', age=40, phone='null', score=0, salary=0}, "
////                    + "User{name='Kate', age=30, phone='null', score=0, salary=0}, "
////                    + "User{name='Luke', age=20, phone='null', score=0, salary=0}, "
////                    + "User{name='Sophia', age=10, phone='null', score=0, salary=0}, "
////                    + "User{name='Steven', age=60, phone='null', score=0, salary=0}, "
////                    + "User{name='Bob', age=30, phone='null', score=0, salary=0}, "
////                    + "User{name='Daniel', age=40, phone='null', score=0, salary=0}]"));
////
////    // selection + projection
//    checkerList.add(Checker.build()
////            .sql("select name,age from user where score = 80")
//            .sql("show tables from test")
////            .sql("show databases")
//            .addCheckPoint(CheckPoint.ExecuteSql, ""
//                    + "[User{name='Kate', age=30, phone='null', score=0, salary=0}, "
//                    + "User{name='Sophia', age=10, phone='null', score=0, salary=0}]"));
////
////    //projection+indexSource
////    checkerList.add(Checker.build()
////            //"select name,age from user where age > 50"   nonsupport
////            .sql("select name,age from user where age = 50")
////            .addCheckPoint(CheckPoint.ExecuteSql, ""
////                    + "[User{name='Mary', age=50, phone='null', score=0, salary=0}]"));
//
//    return checkerList;
//  }
//
//  public static ValueAccessor[] getRows(ColumnExpr[] resultColumns){
//    List<ValueAccessor> rows = new ArrayList<>();
//    for (MockTableData.User user : userList){
//      Value[] colValues = new Value[resultColumns.length];
//      RowValueAccessor row = new RowValueAccessor(colValues);
//      colValues[0] = BinaryValue.getInstance(user.getName().getBytes());
//      colValues[1] = LongValue.getInstance(user.getAge());
////      colValues[2] = BinaryValue.getInstance(user.getPhone().getBytes());
////      colValues[3] = LongValue.getInstance(user.getScore());
////      colValues[4] = LongValue.getInstance(user.getSalary());
//      row.setValues(colValues);
//      rows.add(row);
//    }
//    return rows.toArray(new ValueAccessor[0]);
//  }
//
//  //mock ds return data
//  //limit
//  private static void loadLimitData(){
//    userList.add(new MockTableData.User("Tom", 30,null,0,0));
//    userList.add(new MockTableData.User("Jack", 20,null,0,0));
//    userList.add(new MockTableData.User("Mary", 50,null,0,0));
//    userList.add(new MockTableData.User("Suzy", 40,null,0,0));
//    userList.add(new MockTableData.User("Kate", 30,null,0,0));
//    userList.add(new MockTableData.User("Luke", 20,null,0,0));
//    userList.add(new MockTableData.User("Sophia", 10,null,0,0));
//    userList.add(new MockTableData.User("Steven", 60,null,0,0));
//    userList.add(new MockTableData.User("Bob", 30,null,0,0));
//    userList.add(new MockTableData.User("Daniel", 40,null,0,0));
//  }
//
//  //order by
//  private static void loadOrderData(){
//    userList.add(new MockTableData.User("Tom", 30, "13010010000", 90, 5000));
//    userList.add(new MockTableData.User("Jack", 20, "13010011111", 100, 6000));
//    userList.add(new MockTableData.User("Mary", 50, "13010012222", 60, 7000));
//    userList.add(new MockTableData.User("Suzy", 40, "13010013333", 90, 8000));
//    userList.add(new MockTableData.User("Kate", 30, "13010014444", 80, 9000));
//    userList.add(new MockTableData.User("Luke", 20, "13010015555", 100, 5000));
//    userList.add(new MockTableData.User("Sophia", 10, "13010016666", 80, 4000));
//    userList.add(new MockTableData.User("Steven", 60, "13010017777", 50, 3000));
//    userList.add(new MockTableData.User("Bob", 30, "13010018888", 40, 2000));
//    userList.add(new MockTableData.User("Daniel", 40, "13010019999", 70, 5000));
//  }
//
//  //selection
//  private static void loadSelection(){
//    userList.add(new MockTableData.User("Kate", 30, null, 80, 0));
//    userList.add(new MockTableData.User("Sophia", 10, null, 80, 0));
//  }
//
//  //indexSource "select" return data
//  private static void loadIndexSourceData(){
//    userList.add(new MockTableData.User("Mary",50,null,0,0));
//  }
//}
