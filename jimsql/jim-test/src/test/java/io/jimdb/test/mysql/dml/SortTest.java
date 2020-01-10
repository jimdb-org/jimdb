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
package io.jimdb.test.mysql.dml;

import java.util.Arrays;
import java.util.Random;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.Order;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import org.junit.Test;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

/**
 * @version V1.0
 */
public class SortTest {

  private static final int[] rowsLengthArray = new int[]{ 10, 1000, 2000, 3000, 4000, 5000, 6000, 8192, 1_0000, 10_0000, 100_0000 };

  private static Order.OrderExpression[] expressions;

  static {
    ColumnExpr columnExpr = new ColumnExpr(0L);
    columnExpr.setOffset(0);
    columnExpr.setResultType(Types.buildSQLType(Basepb.DataType.BigInt));

    ColumnExpr columnExpr1 = new ColumnExpr(1L);
    columnExpr1.setOffset(1);
    columnExpr1.setResultType(Types.buildSQLType(Basepb.DataType.Double));

    ColumnExpr columnExpr2 = new ColumnExpr(2L);
    columnExpr2.setOffset(2);
    columnExpr2.setResultType(Types.buildSQLType(Basepb.DataType.Varchar));

    expressions = new Order.OrderExpression[]{
            new Order.OrderExpression(columnExpr, SQLOrderingSpecification.DESC),
            new Order.OrderExpression(columnExpr1, SQLOrderingSpecification.DESC),
            new Order.OrderExpression(columnExpr2, SQLOrderingSpecification.DESC),
    };
  }

  private static void printResult(QueryExecResult result) {
    for (int j = 0; j < result.size(); j++) {
      System.out.println(
              "row " + j + ": " +
                      "col " + 0 + " = " + result.getRow(j).get(0).getString() +
                      " , col " + 1 + " = " + result.getRow(j).get(1).getString() +
                      " , col " + 2 + " = " + result.getRow(j).get(2).getString()
      );
    }
  }

  private static String getRandomString(int length) {
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(62);
      sb.append(str.charAt(number));
    }
    return sb.toString();
  }

  /**
   * Order by Multiple
   */
  public static ExecResult orderByParallel(ExecResult result, Order.OrderExpression[] orderExpressions) {
    result.accept(rows -> Arrays.parallelSort(rows, (o1, o2) -> OperatorUtil.compare(null, orderExpressions, o1, o2)));
    return result;
  }

  /**
   * Order by single
   */
  public static ExecResult orderBy(ExecResult result, Order.OrderExpression[] orderExpressions) {
    result.accept(rows -> Arrays.sort(rows, (o1, o2) -> OperatorUtil.compare(null, orderExpressions, o1, o2)));
    return result;
  }

  private QueryExecResult mockData(int length) {
    RowValueAccessor[] rows = new RowValueAccessor[length];
    Random random = new Random();
    for (int j = 0; j < length; j++) {
      Value[] values = new Value[]{
              LongValue.getInstance(random.nextLong()),
              // LongValue.getInstance(random.nextLong()%3),
              DoubleValue.getInstance(random.nextDouble()),
              // DoubleValue.getInstance(1.0),
              StringValue.getInstance(getRandomString(10))
      };
      RowValueAccessor row = new RowValueAccessor(values);
      rows[j] = row;
    }

    return new QueryExecResult(null, rows);
  }

  @Test
  public void testSort10Times() {
    executeSortTimes(10);
  }

  @Test
  public void testParallelSort10Times() {
    executeParallelSortTimes(10);
  }

  @Test
  public void testSort100Times() {
    executeSortTimes(100);
  }

  @Test
  public void testParallelSort100Times() {
    executeParallelSortTimes(100);
  }

  private void executeSortTimes(int times) {
    for (int i = 0; i < rowsLengthArray.length; i++) {
      int length = rowsLengthArray[i];
      QueryExecResult param = mockData(length), result;
      long start = System.currentTimeMillis();
      for (int j = 0; j < times; j++) {
        result = (QueryExecResult) orderBy(param, expressions);
      }
      long elapse = (System.currentTimeMillis() - start) / times;
      System.out.println("Sequential sort times : " + times + " , rows = " + length + " , sort elapse ： " + elapse + " ms ");
      System.out.println("------------------------------------------------------------------------------------------");
//      printResult(result);
    }
  }

  private void executeParallelSortTimes(int times) {
    for (int i = 0; i < rowsLengthArray.length; i++) {
      int length = rowsLengthArray[i];
      QueryExecResult param = mockData(length), result;
      long start = System.currentTimeMillis();
      for (int j = 0; j < times; j++) {
        result = (QueryExecResult) orderByParallel(param, expressions);
      }
      long elapse = (System.currentTimeMillis() - start) / times;
      System.out.println("Parallel sort times : " + times + " , rows = " + length + " , sort elapse ： " + elapse + " ms ");
      System.out.println("------------------------------------------------------------------------------------------");
//      printResult(result);
    }
  }
}
