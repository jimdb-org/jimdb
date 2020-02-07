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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Processorpb;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.physical.PushDownBuilder;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public class IndexSource extends RelOperator {
  private Table table;
  private KeyValueRange keyValueRange;
  private List<Processorpb.Processor.Builder> processors;
  private RelOperator pushDownIndexPlan;
  private List<Expression> accessConditions;

  // TODO get rid of KeyValueRange
  public IndexSource(Table table, KeyValueRange keyValueRange, Schema schema, OperatorStatsInfo statInfo) {
    this.table = table;
    this.keyValueRange = keyValueRange;
    this.schema = schema;
    this.statInfo = statInfo;
  }

  public IndexSource(IndexSource indexSource, RelOperator indexPlan) {
    this(indexSource);

    this.pushDownIndexPlan = indexPlan;
    this.schema = indexPlan.getSchema();
    this.statInfo = indexPlan.getStatInfo();
  }

  public IndexSource(IndexSource indexSource) {
    this.table = indexSource.table;
    this.keyValueRange = indexSource.keyValueRange;
    this.processors = indexSource.processors;
    this.accessConditions = indexSource.accessConditions;
    this.pushDownIndexPlan = indexSource.pushDownIndexPlan;
    this.copyBaseParameters(indexSource);
    this.schema = indexSource.getSchema();
  }

  private void ready() {
    if (pushDownIndexPlan == null) {
      pushDownIndexPlan = this;
    }
    if (processors == null) {
      processors = createProcessors();
    }
  }

  private List<Processorpb.Processor.Builder> createProcessors() {
    if (pushDownIndexPlan == null) {
      pushDownIndexPlan = this;
    }
    return PushDownBuilder.constructProcessors(pushDownIndexPlan);
  }

  @Override
  public IndexSource copy() {
    IndexSource indexSource = new IndexSource(this);
    indexSource.children = this.copyChildren();

    return indexSource;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    ready();
    int size = getSchema().getColumns().size();
    List<Integer> outputOffsets = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      outputOffsets.add(i);
    }

    return session.getTxn().select(keyValueRange.getIndex(), processors,
            this.schema.getColumns().toArray(new ColumnExpr[size]), outputOffsets, keyValueRange.getValueRanges());
//    return mockDatas;
  }

//  private static Flux<ExecResult> mockDatas;
//
//  static {
//    mockData();
//  }
//
//  private static Flux<ExecResult> mockData() {
//    int size = 10000;
//    int mod = 1000;
//    RowValueAccessor[] rows = new RowValueAccessor[size];
//    for (int i = 0; i < size; i++) {
//      Value[] values = new Value[1];
//      values[0] = LongValue.getInstance(i % mod);
//      rows[i] = new RowValueAccessor(values);
//    }
//
//    ColumnExpr[] columnExprs = new ColumnExpr[1];
//    columnExprs[0] = new ColumnExpr(1L);
//
//    mockDatas = Flux.just(new QueryExecResult(columnExprs, rows));
//    return mockDatas;
//  }

  @Override
  public void resolveOffset() {
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public KeyValueRange getKeyValueRange() {
    return keyValueRange;
  }

  public List<Processorpb.Processor.Builder> getProcessors() {
    return createProcessors();
  }

  public String getExplainInfo() {
    String explainInfo = "";
    if (keyValueRange != null) {
      explainInfo = keyValueRange.getIndex().getName();
    }
    return explainInfo;
  }

  public void setPushDownIndexPlan(RelOperator pushDownIndexPlan) {
    this.pushDownIndexPlan = pushDownIndexPlan;
  }

  public RelOperator getPushDownIndexPlan() {
    return pushDownIndexPlan;
  }

  public List<Expression> getAccessConditions() {
    return accessConditions;
  }

  public void setAccessConditions(List<Expression> accessConditions) {
    this.accessConditions = accessConditions;
  }

  public String getName() {
    return "IndexSource";
  }

  @Override
  @SuppressFBWarnings({ "ITC_INHERITANCE_TYPE_CHECKING", "UCPM_USE_CHARACTER_PARAMETERIZED_METHOD" })
  public String toString() {
    StringBuilder sb = new StringBuilder("IndexSource={");

    // columns
    sb.append("columns=[");
    for (ColumnExpr column : getSchema().getColumns()) {
      sb.append(column.getOriCol() != null ? column.getOriCol() : column.getAliasCol());
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1).append(']');
    if (accessConditions != null && !accessConditions.isEmpty()) {
      sb.append(",condition=[");
      for (Expression expression : accessConditions) {
        sb.append(expression);
        sb.append(',');
      }
    }
    sb.deleteCharAt(sb.length() - 1).append(']');
    sb.append('}');
    return sb.toString();
  }
}
