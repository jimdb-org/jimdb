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
package io.jimdb.sql.optimizer.logical;

import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.OperatorVisitor;

/**
 * Operator visitor for logical plan optimizations
 */
public class LogicalOptimizationVisitor extends OperatorVisitor<RelOperator> {

  // you can override this logic in your extension class when needed
  public RelOperator visitChildren(RelOperator operator) {
    RelOperator[] childrenOperators = operator.getChildren();
    if (null != childrenOperators && childrenOperators.length > 0) {
      for (RelOperator childOperator : childrenOperators) {
        childOperator.acceptVisitor(this);
      }
    }
    return operator;
  }

  @Override
  public RelOperator visitOperatorByDefault(RelOperator operator) {
    return visitChildren(operator);
  }
}
