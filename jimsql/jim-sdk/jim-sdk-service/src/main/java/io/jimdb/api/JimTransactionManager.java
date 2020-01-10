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
package io.jimdb.api;

import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.store.Transaction;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class JimTransactionManager {
  private Catalog db;

  private boolean autoCommit = true;
  private Transaction txn;

  public JimTransactionManager(Catalog db, Transaction txn) {
    this.db = db;
    this.txn = txn;
  }

  private ExecResult getExecResult(Flux<ExecResult> flux) throws Exception {
    Throwable[] t = new Throwable[1];
    ExecResult execResult = flux.doOnError(e -> {
      t[0] = e;
    }).blockFirst();

    if (t[0] != null) {
      throw new Exception(t[0]);
    }
    return execResult;
  }

//  public int insert(String tableName, String[] columnNames, List<Object[]> recordList) throws Exception {
//    Table table = db.getTable(tableName);
//    checkCloumn(table, columnNames);
//    Flux<ExecResult> flux = txn.insert(table, ManagerHelper.convertToColumns(table, columnNames), recordList);
//    ExecResult execResult = getExecResult(flux);
//
//    if (autoCommit) {
//      syncCommit();
//    }
//    return getAffectedRows(execResult);
//  }
//
//  public int update(String tableName, Map<String, Object> targetParams, Object[] ids) throws Exception {
//    Table table = db.getTable(tableName);
//
//    Flux<ExecResult> flux = scanByIds(tableName, null, ids).flatMap(r -> {
//      QueryExecResult rs = (QueryExecResult) r;
//      QueryExecResult.Row[] rows = rs.getRows();
//      if (ManagerHelper.isEmpty(rows)) {
//        return Flux.just(new DMLExecResult(0));
//      }
//      Assignment[] assignments = getAssignments(table, targetParams);
//      return txn.update(table, assignments, rows);
//    });
//
//    ExecResult execResult = getExecResult(flux);
//
//    if (autoCommit) {
//      syncCommit();
//    }
//
//    return getAffectedRows(execResult);
//  }
//
//  public int delete(String tableName, Object[] ids) throws Exception {
//    Table table = db.getTable(tableName);
//    Flux<ExecResult> flux = scanByIds(tableName, null, ids).flatMap(r -> {
//      return delete(table, (QueryExecResult) r);
//    });
//
//    ExecResult execResult = getExecResult(flux);
//
//    if (autoCommit) {
//      syncCommit();
//    }
//
//    return getAffectedRows(execResult);
//  }
//
//  public int delete(String tableName, Exprpb.Expr expr) throws Exception {
//    Table table = db.getTable(tableName);
//    Flux<ExecResult> flux = scan(tableName, null, expr, null).flatMap(r -> {
//      return delete(table, (QueryExecResult) r);
//    });
//
//    ExecResult execResult = getExecResult(flux);
//
//    if (autoCommit) {
//      syncCommit();
//    }
//
//    return getAffectedRows(execResult);
//  }
//
//  private Publisher<? extends ExecResult> delete(Table table, QueryExecResult rs) {
//    QueryExecResult.Row[] rows = rs.getRows();
//    if (ManagerHelper.isEmpty(rows)) {
//      return Flux.just(new DMLExecResult(0));
//    }
//    return txn.delete(table, null, rows);
//  }
//
//  public ExecResult get(String tableName, String[] returnColumnNames, Object id) throws Exception {
//    Flux<ExecResult> flux = getById(tableName, returnColumnNames, id);
//    return getExecResult(flux);
//  }
//
//  public ExecResult select(String tableName, String[] returnColumnNames, Object[] ids) throws Exception {
//    Flux<ExecResult> flux = scanByIds(tableName, returnColumnNames, ids);
//    return getExecResult(flux);
//  }
//
//  public ExecResult select(String tableName, String[] returnColumnNames, Exprpb.Expr expr, int pageIndex,
//                           int pageSize) throws Exception {
//    int offset = pageIndex * pageSize;
//    Txn.Limit limit = Txn.Limit.newBuilder().setOffset(offset).setCount(pageSize).build();
//    Flux<ExecResult> flux = scan(tableName, returnColumnNames, expr, limit);
//    return getExecResult(flux);
//  }
//
//  private Flux<ExecResult> scan(String tableName, String[] returnColumnNames, Exprpb.Expr expr, Txn.Limit limit) {
//    Table table = db.getTable(tableName);
//    ResultColumn[] resultColumns = buildResultColumns(table, returnColumnNames);
//    return txn.scan(table, resultColumns, expr, null, limit);
//  }
//
//  public long count(String tableName, Exprpb.Expr expr) throws Exception {
//    //todo
//    return 0;
//  }
//
//  public void commit() throws Exception {
//    if (!autoCommit) {
//      syncCommit();
//    }
//  }
//
//  private void syncCommit() throws Exception {
//    Flux<ExecResult> flux = txn.commit();
//    getExecResult(flux);
//  }
//
//  public void rollback() {
//    if (!autoCommit) {
//      txn.rollback();
//    }
//  }
//
//  public void setAutoCommit(boolean autoCommit) {
//    if (!this.autoCommit && autoCommit) {
//      txn.commit();
//    }
//    this.autoCommit = autoCommit;
//  }
//
//  private Flux<ExecResult> scanByIds(String tableName, String[] returnColumnNames, Object[] ids) throws Exception {
//    Table table = db.getTable(tableName);
//    ResultColumn[] resultColumns = buildResultColumns(table, returnColumnNames);
//    Index pk = getPK(table);
//    return txn.scan(table, resultColumns, getExprByIds(pk.getColumns()[0], ids), null, null);
//  }
//
//  private Flux<ExecResult> getById(String tableName, String[] returnColumnNames, Object id) throws Exception {
//    Table table = db.getTable(tableName);
//    ResultColumn[] resultColumns = buildResultColumns(table, returnColumnNames);
//    Index pk = getPK(table);
//    return txn.getByIndex(resultColumns, pk, new Object[]{id});
//  }
//
//  public Column getColumn(String tableName, String column) {
//    Table table = db.getTable(tableName);
//    return table.getColumn(column);
//  }
//
//  private Index getPK(Table table) throws Exception {
//    for (Index index : table.getIndexes()) {
//      if (index.isPrimary()) {
//        return index;
//      }
//    }
//    throw new Exception("can not find primary key.");
//  }
//
//  private Exprpb.Expr getExprByIds(Column column, Object[] ids) {
//    Exprpb.Expr.Builder resultBuilder = Exprpb.Expr.newBuilder();
//    for (Object id : ids) {
//      List<Exprpb.Expr> exprList = new ArrayList<>();
//      exprList.add(Exprpb.Expr.newBuilder()
//              .setExprType(Exprpb.ExprType.Column)
//              .setColumn(Converter.convertColumn(column)).build());
//      exprList.add(Exprpb.Expr.newBuilder()
//              .setExprType(Exprpb.ExprType.Const_Int)
//              .setValue(Converter.encodeValue(id)).build());
//      Exprpb.Expr expr = Exprpb.Expr.newBuilder()
//              .setExprType(Exprpb.ExprType.Equal)
//              .addAllChild(exprList)
//              .build();
//      resultBuilder.addChild(expr);
//    }
//
//    return resultBuilder.setExprType(Exprpb.ExprType.LogicOr).build();
//  }
//
//  private void checkCloumn(Table table, String[] columnNames) throws Exception {
//    for (String columnName : columnNames) {
//      if (table.getColumn(columnName) == null) {
//        throw new Exception("column name [" + columnName + "] is not correct");
//      }
//    }
//  }
//
//  private Assignment[] getAssignments(Table table, Map<String, Object> params) {
//    Assignment[] assignments = new Assignment[params.size()];
//    int counter = 0;
//    for (Map.Entry<String, Object> entry : params.entrySet()) {
//      String columnName = entry.getKey();
//      Object columnValue = entry.getValue();
//      assignments[counter] = getAssignment(table, columnName, columnValue);
//      counter++;
//    }
//    return assignments;
//  }
//
//  private Assignment getAssignment(Table table, String columnName, Object columnValue) {
//    Column column = table.getColumn(columnName);
//    ResultColumn resultColumn = buildResultColumn(table, column);
//    return new Assignment(resultColumn, getValue(column, columnValue));
//  }
//
//  private SQLExpr getValue(Column column, Object columnValue) {
//    SQLExpr value;
//    switch (column.getType()) {
//      case Varchar:
//        value = new SQLCharExpr(columnValue.toString());
//        break;
//      case Int:
//        value = new SQLIntegerExpr((Integer) columnValue);
//        break;
//      case BigInt:
//        value = new SQLIntegerExpr((Long) columnValue);
//        break;
//      default:
//        throw new RuntimeException("column type is no support");
//    }
//    return value;
//  }
//
//  private ResultColumn[] buildResultColumns(Table t, String[] columnNames) {
//    Column[] columns = ManagerHelper.convertToColumns(t, columnNames);
//    if (ManagerHelper.isEmpty(columns)) {
//      columns = t.getColumns();
//    }
//
//    ResultColumn[] resultColumns = new ResultColumn[columns.length];
//    for (int i = 0; i < columns.length; i++) {
//      resultColumns[i] = buildResultColumn(t, columns[i]);
//    }
//    return resultColumns;
//  }
//
//  private ResultColumn buildResultColumn(Table table, Column column) {
//    return new ResultColumn.ResultColumnBuilder(column, column.getName(), column.getType(), column.getOffset())
//            .catalog(table.getCatalog().getName())
//            .oriTable(table.getName()).build();
//  }
//
//  private int getAffectedRows(ExecResult execResult) {
//    return execResult == null ? 0 : (int) execResult.getAffectedRows();
//  }
}
