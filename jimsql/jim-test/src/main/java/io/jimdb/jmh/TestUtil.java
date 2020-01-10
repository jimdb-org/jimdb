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
package io.jimdb.jmh;

import java.util.Random;

/**
 * @version V1.0
 */
public class TestUtil {

  private static final String DATA_STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//
//  public static ResultColumn[] convertToResultColumn(Table t) {
//    Column[] columns = t.getColumns();
//    ResultColumn[] resultColumns = new ResultColumn[columns.length];
//    for (int i = 0; i < columns.length; i++) {
//      Column col = columns[i];
//      resultColumns[i] = buildResultColumn(t, col);
//    }
//    return resultColumns;
//  }
//
//  private static ResultColumn buildResultColumn(Table table, Column column) {
//    return new ResultColumn.ResultColumnBuilder(column, column.getName(), column.getType(), column.getOffset())
//            .catalog(table.getCatalog().getName())
//            // FIXME why not use the aliasTable?
//            .aliasTable(table.getName())
//            .oriTable(table.getName()).build();
//  }
//
//  public static AssignmentV1[] buildUpdateList(Table table, Object[] objects) {
//    Column[] columns = table.getColumns();
//    Column[] pkCols = table.getPrimary();
//    AssignmentV1[] assignments = new AssignmentV1[columns.length - pkCols.length];
//    int i = 0;
//    for (Column column : columns) {
//      if (column.isPrimary()) {
//        continue;
//      }
//      ResultColumn resultColumn = buildResultColumn(table, column);
//      SQLExpr value;
//      switch (column.getType()) {
//        case Varchar:
//          value = new SQLCharExpr(objects[i].toString());
//          break;
//        case Tinyint:
//        case Smallint:
//        case Int:
//        case BigInt:
//          value = new SQLIntegerExpr((Long) objects[i]);
//          break;
//        default:
//          throw new RuntimeException("no support");
//      }
//      assignments[i] = new AssignmentV1(resultColumn, value);
//      i++;
//    }
//    return assignments;
//  }
//
//  public static AssignmentV1[] buildUpdateListForPart(Table table, Object[] objects) {
//    Column[] columns = table.getColumns();
//
//    AssignmentV1[] assignments = new AssignmentV1[1];
//    for(Column column : columns) {
//      if (!column.getName().equals("v")) {
//        continue;
//      }
//      ResultColumn resultColumn = buildResultColumn(table, column);
//      SQLExpr value;
//      switch (column.getType()) {
//        case Varchar:
//          value = new SQLCharExpr(objects[0].toString());
//          break;
//        case Tinyint:
//        case Smallint:
//        case Int:
//        case BigInt:
//          value = new SQLIntegerExpr((Long) objects[0]);
//          break;
//        default:
//          throw new RuntimeException("no support");
//      }
//      assignments[0] = new AssignmentV1(resultColumn, value);
//    }
//    return assignments;
//  }

  public static String getRandomString(int length) {
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(62);
      sb.append(DATA_STR.charAt(number));
    }
    return sb.toString();
  }
}
