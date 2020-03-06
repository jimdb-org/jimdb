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
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Processorpb;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.physical.DSTask;
import io.jimdb.core.values.Value;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class IndexLookup extends RelOperator {

  private RelOperator pushedDownTablePlan;

  private RelOperator pushedDownIndexPlan;

  private TableSource tableSource;

  private IndexSource indexSource;

  public IndexLookup(final DSTask dsTask) {
    this.pushedDownTablePlan = dsTask.getPushedDownTablePlan();
    this.pushedDownIndexPlan = dsTask.getPushedDownIndexPlan();
//    this.schema = pushedDownTablePlan.getSchema().clone();
    this.schema = pushedDownTablePlan.getSchema();
    this.statInfo = pushedDownTablePlan.getStatInfo();

    if (dsTask.getTableSource() != null) {
      this.tableSource = new TableSource(dsTask.getTableSource());
      this.tableSource.setPushDownTablePlan(this.pushedDownTablePlan);
      this.tableSource.setSchema(this.schema);
    }

    if (dsTask.getIndexSource() != null) {
      this.indexSource = new IndexSource(dsTask.getIndexSource());
      this.indexSource.setPushDownIndexPlan(this.pushedDownIndexPlan);
    }
  }

  private IndexLookup(IndexLookup indexLookup) {
    this.pushedDownTablePlan = indexLookup.pushedDownTablePlan;
    this.pushedDownIndexPlan = indexLookup.pushedDownIndexPlan;
    this.tableSource = indexLookup.tableSource;
    this.indexSource = indexLookup.indexSource;
    this.schema = indexLookup.schema;
    //this.tableSource.setSchema(this.schema);
    this.copyBaseParameters(indexLookup);
  }

  @Override
  public IndexLookup copy() {
    IndexLookup indexLookup = new IndexLookup(this);
    indexLookup.children = this.copyChildren();

    return indexLookup;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    return indexSource.execute(session).flatMap(execResult -> {
      if (execResult.size() == 0) {
        List<ColumnExpr> columnExprs = tableSource.getSchema().getColumns();
        return Flux.just(new QueryExecResult(columnExprs.toArray(new ColumnExpr[columnExprs.size()]), new ValueAccessor[0]));
      }
      List<ColumnExpr> pkColumnExprs = tableSource.getPKColumnExprs();
      List<ValueRange> valueRangeList = new ArrayList<>(execResult.size());
      execResult.forEach(row -> {
        List<Value> pkVals = new ArrayList<>(pkColumnExprs.size());
        for (ColumnExpr pkColumnExpr : pkColumnExprs) {
          pkVals.add(pkColumnExpr.exec(row));
        }
        valueRangeList.add(new ValueRange(pkVals, pkVals, true, true));
      });
      tableSource.setKeyValueRange(new KeyValueRange(tableSource.getTable().getPrimaryIndex(), pkColumnExprs, valueRangeList));
      return tableSource.execute(session);
    });
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  @Override
  public void resolveOffset() {
    this.tableSource.resolveOffset();

    this.indexSource.resolveOffset();
  }

  public List<Processorpb.Processor.Builder> getTablePlanProcessors() {
    return tableSource.getProcessors();
  }

  public List<Processorpb.Processor.Builder> getIndexPlanProcessors() {
    return indexSource.getProcessors();
  }

  public RelOperator getPushedDownTablePlan() {
    return pushedDownTablePlan;
  }

  public RelOperator getPushedDownIndexPlan() {
    return pushedDownIndexPlan;
  }

  public TableSource getTableSource() {
    return tableSource;
  }

  public IndexSource getIndexSource() {
    return indexSource;
  }

  @Override
  public String getName() {
    return "IndexLookup";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("IndexLookup{");
    sb.append("pushedDownTablePlan=").append(pushedDownTablePlan);
    sb.append(", pushedDownIndexPlan=").append(pushedDownIndexPlan);
    sb.append(", tableSource=").append(tableSource);
    sb.append(", indexSource=").append(indexSource);
    sb.append('}');
    return sb.toString();
  }
}
