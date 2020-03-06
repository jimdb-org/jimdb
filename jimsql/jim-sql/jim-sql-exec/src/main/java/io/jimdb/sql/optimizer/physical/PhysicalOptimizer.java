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
package io.jimdb.sql.optimizer.physical;

import io.jimdb.core.Session;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("CLI_CONSTANT_LIST_INDEX")
public class PhysicalOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalOptimizer.class);

  private static BestTaskFinder bestTaskFinder = new BestTaskFinder();

  /**
   * Entry point to perform physical optimizations
   *
   * @param session     session of the given query
   * @param relOperator the logical plan to be optimized
   * @return optimized physical plan
   */
  public static RelOperator optimize(Session session, RelOperator relOperator) {
    StatisticsVisitor statisticsVisitor = new StatisticsVisitor();
    OperatorStatsInfo statInfo = statisticsVisitor.deriveStatInfo(session, relOperator);

    if (statInfo == null) {
      LOG.debug("Could not derive stats info in PhysicalOptimizer ...");
    }

    // decorate access path before building stats
    AccessPathDecorator.decorateAccessPath(session, relOperator);

    Task oldTask = relOperator.acceptVisitor(session, bestTaskFinder, PhysicalProperty.DEFAULT_PROP);
    Task task = oldTask.finish();

    final RelOperator result = task.getPlan();
    result.resolveOffset();

    // build aggregation executor
    // FIXME why not put this into the agg operator when using bestTaskFinder to visit?
    build(session, result);
    return result;
  }

  private static void build(Session session, RelOperator plan) {
    RelOperator operator = plan;
    do {
      if (operator instanceof Aggregation) {
        ((Aggregation) operator).buildExecutor(session);
        break;
      }
    } while (operator.getChildren() != null && (operator = operator.getChildren()[0]) != null);
  }

}
