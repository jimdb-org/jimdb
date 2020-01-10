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
package io.jimdb.sql.optimizer.logical;

import io.jimdb.core.Session;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.OptimizeFlag;

/**
 * Entry class for logical optimization (to optimize a logical plan tree).
 */
public final class LogicalOptimizer {
  private static final int[] LOGICAL_RULES;

  static {
    LOGICAL_RULES = new int[]{
            OptimizeFlag.PRUNCOLUMNS, // done -> testing
            OptimizeFlag.BUILDKEYINFO, // done -> testing
            OptimizeFlag.DECORRELATE,
            OptimizeFlag.ELIMINATEAGG, // done -> testing
            OptimizeFlag.ELIMINATEPROJECTION, // done -> testing
            OptimizeFlag.MAXMINELIMINATE, // todo later
            OptimizeFlag.PREDICATEPUSHDOWN, // done -> testing
            OptimizeFlag.ELIMINATEOUTERJOIN,
            OptimizeFlag.PARTITIONPROCESSOR,
            OptimizeFlag.PUSHDOWNAGG, // todo later
            OptimizeFlag.PUSHDOWNTOPN, // done -> testing
            OptimizeFlag.JOINREORDER
    };
  }

  public static RelOperator optimize(RelOperator relOperator, final int optimizationFlag, Session session) {
    for (int rule : LOGICAL_RULES) {
      if ((rule & optimizationFlag) != 0) {
//        System.out.println("rule -> " + rule);
//        if (rule == OptimizeFlag.PREDICATEPUSHDOWN) {
//          continue;
//        }
        IRuleOptimizer ruleOptimizer = RuleOptimizerBuilder.build(rule);
        if (ruleOptimizer == null) {
          continue;
        }
        relOperator = ruleOptimizer.optimize(relOperator, session);
      }
    }
    return relOperator;
  }
}
