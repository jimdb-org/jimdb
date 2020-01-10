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
package io.jimdb.sql.operator;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.sql.optimizer.OperatorVisitor;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.physical.PhysicalProperty;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "ITC_INHERITANCE_TYPE_CHECKING" })
public abstract class RelOperator extends Operator {
  protected boolean isMaxOneRow;
  protected RelOperator[] children;

  protected OperatorStatsInfo statInfo;
  protected PhysicalProperty physicalProperty = PhysicalProperty.DEFAULT_PROP;

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.LOGIC;
  }

  // TODO Ideally we should make this abstract, but doing so will need all the RelOperator implements this method,
  //  including those show operators
  public RelOperator copy() {
    return this;
  }

  public RelOperator[] copyChildren() {
    if (this.children == null) {
      return null;
    }

    RelOperator[] children = new RelOperator[this.children.length];
    for (int i = 0; i < this.children.length; i++) {
      children[i] = this.children[i].copy();
    }

    return children;
  }

  public RelOperator[] getChildren() {
    return children;
  }

  public void setChildren(RelOperator... children) {
    this.children = children;
  }

  public void setChild(int index, RelOperator child) {
    this.children[index] = child;
  }

  public boolean isMaxOneRow() {
    return isMaxOneRow;
  }

  public void setIsMaxOneRow(boolean maxOneRow) {
    this.isMaxOneRow = maxOneRow;
  }

  public boolean hasChildren() {
    return children != null && children[0] != null;
  }

  /**
   * getCandidatesProperties :  get the possible properties ready.
   *
   * @return
   */
  public ColumnExpr[][] getCandidatesProperties() {
    if (hasChildren()) {
      for (RelOperator ch : this.getChildren()) {
        ch.getCandidatesProperties();
      }
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    if (schema == null && hasChildren()) {
      return children[0].getSchema();
    }

    return super.getSchema();
  }

  public void copyBaseParameters(RelOperator operator) {
    this.isMaxOneRow = operator.isMaxOneRow;
    this.schema = operator.schema;
    this.statInfo = operator.statInfo;
  }

  public <T, R> R acceptVisitor(Session session, ParameterizedOperatorVisitor<T, R> visitor, T t) {
    return visitor.visitOperator(session, this, t);
  }

  @Override
  public <R> R acceptVisitor(OperatorVisitor<R> visitor) {
    return visitor.visitOperator(this);
  }

  public OperatorStatsInfo getStatInfo() {
    return statInfo;
  }

  public void setStatInfo(OperatorStatsInfo statInfo) {
    this.statInfo = statInfo;
  }

  public PhysicalProperty getPhysicalProperty() {
    return physicalProperty;
  }

  public void setPhysicalProperty(PhysicalProperty physicalProperty) {
    this.physicalProperty = physicalProperty;
  }

  protected String export(Expression[] expressions) {
    if (expressions != null && expressions.length > 0) {
      StringBuilder sb = new StringBuilder("[");
      for (Expression expression : expressions) {
        if (expression instanceof ValueExpr) {
          ValueExpr ve = (ValueExpr) expression;
          sb.append(ve.getExprType()).append('-').append(ve.getValue());
        } else if (expression instanceof ColumnExpr) {
          ColumnExpr ce = (ColumnExpr) expression;
          sb.append(ce.getExprType());
          if (ce.getOriCol() != null) {
            sb.append('-');
            sb.append(ce.getOriCol());
          }
          if (ce.getAliasCol() != null) {
            sb.append('-');
            sb.append(ce.getAliasCol());
          }
        } else if (expression instanceof FuncExpr) {
          FuncExpr fe = (FuncExpr) expression;
          sb.append(fe);
        }
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1).append(']');
      return sb.toString();
    }
    return "";
  }
}
