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
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * explain the plan for physicalPlan
 */
@SuppressFBWarnings({ "ITC_INHERITANCE_TYPE_CHECKING" })
public class Explain extends Operator {
  private static final String TASK_PROXY = "proxy";
  private static final String TASK_DB = "ds";
  private static final String DEFAULT_COUNT = "0";
  private Operator child;
  private boolean analyze;

  public Explain(Operator child) {
    this.child = child;
  }

  public Operator getChild() {
    return child;
  }

  public void setChild(Operator child) {
    this.child = child;
  }

  public boolean isAnalyze() {
    return analyze;
  }

  public void setAnalyze(boolean analyze) {
    this.analyze = analyze;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    ExplainInfo explainInfo = builderExplain(child, TASK_PROXY);
    List<RowValueAccessor> rows = new ArrayList<>();
    renderExplain(explainInfo, rows);

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]),
            rows.toArray(new ValueAccessor[0])));
  }

  /**
   * convert plan
   *
   * @return
   */
  public ExplainInfo builderExplain(Operator plan, String taskName) {
    ExplainInfo explainInfo = new ExplainInfo();
    explainInfo.setOp(plan.getName());
    explainInfo.setTask(taskName);
    if (plan instanceof Delete) {
      Delete delete = (Delete) plan;
      explainInfo.setCount("N/A");
      explainInfo.setOperatorInfo("N/A");
      if (delete.getSelect() != null) {
        explainInfo.addChild(builderExplain(delete.getSelect(), TASK_DB));
      }
      return explainInfo;
    }

    if (plan instanceof Update) {
      Update update = (Update) plan;
      explainInfo.setCount("N/A");
      explainInfo.setOperatorInfo("N/A");
      if (update.getSelect() != null) {
        explainInfo.addChild(builderExplain(update.getSelect(), TASK_DB));
      }
      return explainInfo;
    }

    if (plan instanceof Insert) {
      explainInfo.setCount("N/A");
      explainInfo.setOperatorInfo("N/A");
      return explainInfo;
    }

    if (plan instanceof RelOperator) {
      RelOperator relOperator = (RelOperator) plan;
      explainInfo.setCount(relOperator.getStatInfo() != null
              ? String.valueOf(relOperator.getStatInfo().getEstimatedRowCount()) : DEFAULT_COUNT);
      explainInfo.setOperatorInfo(relOperator.toString());

      if (relOperator.getChildren() != null && relOperator.getChildren().length > 0) {
        RelOperator[] children = relOperator.getChildren();
        for (RelOperator child : children) {
          explainInfo.addChild(builderExplain(child, TASK_DB.equals(explainInfo.getTask()) ? TASK_DB : TASK_PROXY));
        }
      }
    }

    if (this.isAnalyze()) {
      // TODO analyze execute time
    }
    if (plan instanceof TableSource) {
      TableSource tableSource = (TableSource) plan;
      if (tableSource.getPushDownTablePlan() != null) {
        explainInfo.addChild(builderExplain(tableSource.getPushDownTablePlan(), TASK_DB));
      }
    }
    if (plan instanceof IndexSource) {
      IndexSource indexSource = (IndexSource) plan;
      if (indexSource.getPushDownIndexPlan() != null) {
        explainInfo.addChild(builderExplain(indexSource.getPushDownIndexPlan(), TASK_DB));
      }
    }
    if (plan instanceof IndexLookup) {
      IndexLookup indexLookup = (IndexLookup) plan;
      if (indexLookup.getPushedDownTablePlan() != null) {
        explainInfo.addChild(builderExplain(indexLookup.getPushedDownTablePlan(), TASK_DB));
      }
      if (indexLookup.getPushedDownIndexPlan() != null) {
        explainInfo.addChild(builderExplain(indexLookup.getPushedDownIndexPlan(), TASK_DB));
      }
    }
    return explainInfo;
  }

  /**
   * render explain tree
   *
   * @param explainInfo
   * @param rows
   */
  public void renderExplain(ExplainInfo explainInfo, List<RowValueAccessor> rows) {
    List<Value> list = new ArrayList<>();
    list.add(StringValue.getInstance(explainInfo.getOp()));
    list.add(StringValue.getInstance(explainInfo.getCount()));
    list.add(StringValue.getInstance(explainInfo.getTask()));
    list.add(StringValue.getInstance(explainInfo.getOperatorInfo()));
    if (this.isAnalyze()) {
      list.add(StringValue.getInstance(explainInfo.getExecutionInfo()));
      list.add(StringValue.getInstance(explainInfo.getMemory()));
    }

    RowValueAccessor row = new RowValueAccessor(list.toArray(new Value[0]));
    rows.add(row);
    List<ExplainInfo> children = explainInfo.getChildren();
    if (children != null && !children.isEmpty()) {
      children.forEach(child -> renderExplain(child, rows));
    }
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.EXPLAIN;
  }

  /**
   * builder explain result
   */
  private static class ExplainInfo {
    private String op;
    private String count;
    private String task;
    private String operatorInfo;
    private String executionInfo;
    private String memory;
    private List<ExplainInfo> children;

    private ExplainInfo() {
    }

    private ExplainInfo(String op, String count, String task, String operatorInfo) {
      this.op = op;
      this.count = count;
      this.task = task;
      this.operatorInfo = operatorInfo;
    }

    public String getOp() {
      return op;
    }

    public void setOp(String op) {
      this.op = op;
    }

    public String getCount() {
      return count;
    }

    public void setCount(String count) {
      this.count = count;
    }

    public String getTask() {
      return task;
    }

    public void setTask(String task) {
      this.task = task;
    }

    public String getOperatorInfo() {
      return operatorInfo;
    }

    public void setOperatorInfo(String operatorInfo) {
      this.operatorInfo = operatorInfo;
    }

    public String getExecutionInfo() {
      return executionInfo;
    }

    public void setExecutionInfo(String executionInfo) {
      this.executionInfo = executionInfo;
    }

    public String getMemory() {
      return memory;
    }

    public void setMemory(String memory) {
      this.memory = memory;
    }

    public List<ExplainInfo> getChildren() {
      return children;
    }

    public void setChildren(List<ExplainInfo> children) {
      this.children = children;
    }

    public void addChild(ExplainInfo explainInfo) {
      if (children == null) {
        children = new ArrayList<>();
      }
      children.add(explainInfo);
    }
  }
}
