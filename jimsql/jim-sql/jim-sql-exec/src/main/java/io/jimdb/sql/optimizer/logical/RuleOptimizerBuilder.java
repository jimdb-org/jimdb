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

import io.jimdb.sql.optimizer.OptimizeFlag;

/**
 * Builder class to create logical optimizers for different rules.
 */
public final class RuleOptimizerBuilder {
  private RuleOptimizerBuilder() {
  }

  public static IRuleOptimizer build(int ruleType) {
    switch (ruleType) {
      case OptimizeFlag.PRUNCOLUMNS:
        return new ColumnPruneOptimizer();

      case OptimizeFlag.BUILDKEYINFO:
        return new BuildKeyOptimizer();

//      case OptimizeFlag.MAXMINELIMINATE:
//        return new MaxMinEliminateOptimizer();

      case OptimizeFlag.PUSHDOWNTOPN:
        return new TopNPushDownOptimizer();

      case OptimizeFlag.PREDICATEPUSHDOWN:
        return new PredicatesPushDownOptimizer();

      case OptimizeFlag.ELIMINATEPROJECTION:
        return new ProjectionEliminateOptimizer();
      case OptimizeFlag.ELIMINATEAGG:
        return new AggEliminateOptimizer();
    }
    return null;
  }
}
