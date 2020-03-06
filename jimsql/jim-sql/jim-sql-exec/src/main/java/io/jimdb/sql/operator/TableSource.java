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
package io.jimdb.sql.operator;

import static java.util.Collections.EMPTY_LIST;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Processorpb;
import io.jimdb.sql.optimizer.IndexHint;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.physical.PushDownBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TableScanOp represents a scan operator.
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "FCBL_FIELD_COULD_BE_LOCAL", "URF_UNREAD_FIELD" })
public final class TableSource extends RelOperator {
  private String dbName;
  private String aliasName;
  private Table table;
  private Column[] columns;
  private List<IndexHint> hints;

  // FIXME there are too much redundancy here. Ideally we should only store the selected table access path instead of
  //  all of them, and we should not have fields like keyValueRange and accessConditions
  private List<TableAccessPath> tableAccessPaths;
  // primary-key range
  private KeyValueRange keyValueRange;
  private List<Processorpb.Processor.Builder> processors;
  private RelOperator pushDownTablePlan;

  private List<Expression> pushDownPredicates;
  private List<Expression> allPredicates;

  private List<Expression> accessConditions;

  public TableSource(String dbName, String aliasName, Table table, List<IndexHint> hints, Column[] columns,
                     List<TableAccessPath> tableAccessPaths, Schema schema) {
    this.dbName = dbName;
    this.aliasName = aliasName;
    this.table = table;
    this.hints = hints;
    this.columns = columns;
    this.tableAccessPaths = tableAccessPaths;
    this.schema = schema;
  }

  public TableSource(TableSource tableSource, RelOperator tablePlan) {
    this(tableSource);

    this.pushDownTablePlan = tablePlan;
//    this.schema = tablePlan.getSchema().clone();
    this.schema = tablePlan.getSchema();
    this.statInfo = tablePlan.getStatInfo();
  }

  public TableSource(@Nonnull TableSource tableSource) {
    this.dbName = tableSource.dbName;
    this.aliasName = tableSource.aliasName;
    this.table = tableSource.table;
    this.columns = tableSource.columns;
    this.hints = tableSource.hints;
    this.tableAccessPaths = tableSource.tableAccessPaths;
    this.keyValueRange = tableSource.keyValueRange;
    this.processors = tableSource.processors;
    this.pushDownTablePlan = tableSource.pushDownTablePlan;
    this.pushDownPredicates = tableSource.pushDownPredicates;
    this.allPredicates = tableSource.allPredicates;
    this.accessConditions = tableSource.accessConditions;
    this.copyBaseParameters(tableSource);
//    this.schema = tableSource.getSchema().clone();
    this.schema = tableSource.getSchema();
  }

  // FIXME not necessarily needed
  public List<Expression> getAccessConditions() {
    return accessConditions;
  }

  // FIXME not necessarily needed
  public void setAccessConditions(List<Expression> accessConditions) {
    this.accessConditions = accessConditions;
  }

  private List<Processorpb.Processor.Builder> createProcessors() {
    if (pushDownTablePlan == null) {
      pushDownTablePlan = this;
    }
    return PushDownBuilder.constructProcessors(pushDownTablePlan);
  }

  @Override
  public TableSource copy() {
    TableSource tableSource = new TableSource(this);
    tableSource.children = this.copyChildren();

    return tableSource;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    processors = createProcessors();

    List<Integer> outputOffsets = new ArrayList<>(getSchema().getColumns().size());
    for (ColumnExpr columnExpr : this.getSchema().getColumns()) {
      outputOffsets.add(columnExpr.getOffset());
    }

    int size = schema.getColumns().size();
    Flux<ExecResult> result;

    if (keyValueRange.isFullRange()) {
      result = session.getTxn().select(table, processors, schema.getColumns().toArray(new ColumnExpr[size]),
              outputOffsets);
    } else {
      result = session.getTxn().select(keyValueRange.getIndex(), processors, schema.getColumns().toArray(new
              ColumnExpr[size]), outputOffsets, keyValueRange.getValueRanges());
    }

    return result;
  }

  @Override
  public void resolveOffset() {
//    if (this.pushDownTablePlan != null) {
//      this.pushDownTablePlan.resolveOffset();
//    }
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  public String getDbName() {
    return dbName;
  }

  public String getAliasName() {
    return aliasName;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public Column[] getColumns() {
    return columns;
  }

  public void setColumns(Column[] columns) {
    this.columns = columns;
  }

  public List<TableAccessPath> getTableAccessPaths() {
    return tableAccessPaths;
  }

  public void setTableAccessPaths(List<TableAccessPath> tableAccessPaths) {
    this.tableAccessPaths = tableAccessPaths;
  }

  public void setKeyValueRange(KeyValueRange keyValueRange) {
    this.keyValueRange = keyValueRange;
  }

  public List<Processorpb.Processor.Builder> getProcessors() {
    return createProcessors();
  }

  public KeyValueRange getKeyValueRange() {
    return keyValueRange;
  }

  public List<Expression> getPushDownPredicates() {
    return pushDownPredicates;
  }

  public List<Expression> getAllPredicates() {
    return allPredicates;
  }

  public void setPushDownPredicates(List<Expression> pushDownPredicates) {
    this.pushDownPredicates = pushDownPredicates;
  }

  public void setPushDownTablePlan(RelOperator pushDownTablePlan) {
    this.pushDownTablePlan = pushDownTablePlan;
  }

  public void setAllPredicates(List<Expression> allPredicates) {
    this.allPredicates = allPredicates;
  }

  /**
   * Traverse the index to match the possible combinations of index columns
   *
   * @return
   */
  @Override
  public ColumnExpr[][] getCandidatesProperties() {
    List<ColumnExpr[]> result = new ArrayList<>();
    for (TableAccessPath path : tableAccessPaths) {
      List<ColumnExpr> idxColumns = path.getIndexColumns();
      if (idxColumns.isEmpty()) {
        continue;
      }

      // a_b_c   =>  group by a | group by a , b | group by a , b , c
      for (int j = 0; j < idxColumns.size(); j++) {
        ColumnExpr[] possibleIndexCols = new ColumnExpr[j + 1];
        for (int k = 0; k < j + 1; k++) {
          possibleIndexCols[k] = idxColumns.get(k).clone();
        }
        result.add(possibleIndexCols);
      }
    }

    return result.toArray(new ColumnExpr[result.size()][]);
  }

  public RelOperator getPushDownTablePlan() {
    return pushDownTablePlan;
  }

  public String getName() {
    return "TableSource";
  }

  public List<ColumnExpr> getPKColumnExprs() {
    for (TableAccessPath path : this.tableAccessPaths) {
      if (path.isTablePath()) {
        return path.getIndexColumns();
      }
    }
    return EMPTY_LIST;
  }

  @Override
  @SuppressFBWarnings({ "ITC_INHERITANCE_TYPE_CHECKING", "UCPM_USE_CHARACTER_PARAMETERIZED_METHOD" })
  public String toString() {
    StringBuilder sb = new StringBuilder("TableSource={");

    // columns
    sb.append("columns=[");
    if (null != columns) {
      for (Column column : columns) {
        sb.append(column.getName());
        sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("]");

    // push down expressions
    if (pushDownPredicates != null && !pushDownPredicates.isEmpty()) {
      sb.append(", pushDownPredicates=");
      sb.append(export(pushDownPredicates.toArray(new Expression[0])));
    }
    sb.append('}');
    return sb.toString();
  }
}
