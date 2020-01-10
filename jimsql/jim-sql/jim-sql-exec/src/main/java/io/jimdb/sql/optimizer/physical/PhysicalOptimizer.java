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

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

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
    // decorate access path before building stats
//    AccessPathDecorator accessPathDecorator = new AccessPathDecorator(session);
//    if (!relOperator.acceptVisitor(accessPathDecorator)) {
//      // TODO log
//    }

//    updateAccessPath(session, relOperator);

    StatisticsVisitor statisticsVisitor = new StatisticsVisitor();
    OperatorStatsInfo statInfo = statisticsVisitor.deriveStatInfo(session, relOperator);

    if (statInfo == null) {
      LOG.debug("Could not derive stats info in PhysicalOptimizer ...");
    }

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

  private static void updateTablePath(Session session, TableSource tableSource, TableAccessPath path) {
    List<Expression> conditions = tableSource.getPushDownPredicates();

    if (path.getIndex() == null) {
      for (Index index : tableSource.getTable().getReadableIndices()) {
        if (index.isPrimary()) {
          path.setIndex(index);
          break;
        }
      }
      if (path.getIndex() == null) {
        return;
      }
    }

    if (conditions == null || conditions.isEmpty()) {
      path.setRange(RangeBuilder.fullRange());
      return;
    }

    Column pkColumn = path.getIndex().getColumns()[0];
    Tuple2<List<Expression>, List<Expression>> cnfResult =
            NFDetacher.detachConditions(session, conditions, pkColumn.getId());
    path.setAccessConditions(cnfResult.getT1());
    path.setTableConditions(cnfResult.getT2());
    path.setRanges(RangeBuilder.buildPKRange(session, cnfResult.getT1(), pkColumn.getType()));
  }

  private static void updateIndexPath(Session session, TableSource tableSource, TableAccessPath path) {
    List<Expression> conditions = tableSource.getPushDownPredicates();
    if (path.getIndex().isPrimary()
//            || path.getIndex().getColumns().length != 1
    ) {
      return;
    }

    if (conditions == null || conditions.isEmpty()) {
      path.setRange(RangeBuilder.fullRange());
      return;
    }

    // FIXME is this correct? seems the same as the table path?
    Column pkColumn = path.getIndex().getColumns()[0];
    Tuple2<List<Expression>, List<Expression>> cnfResult = NFDetacher.detachConditions(session, conditions, pkColumn.getId());
    if (cnfResult.getT1().isEmpty()) {
      return;
    }
    path.setAccessConditions(cnfResult.getT1());
    path.setTableConditions(cnfResult.getT2());
    List<ValueRange> ranges = RangeBuilder.buildColumnRange(session, cnfResult.getT1(), pkColumn.getType());
    path.setRanges(ranges);
  }

  /**
   * update TableAccessPath : detach range and push-down condition
   * TODO This is not the right place to update the access path, consider to put this logical inside pruningAccessPath
   */
  private static void updateAccessPath(Session session, RelOperator operator) {
    TableSource tableSource = null;

    // find table source
    while (operator.hasChildren()) {
      operator = operator.getChildren()[0];
    }
    if (operator instanceof TableSource) {
      tableSource = (TableSource) operator;
    }

    if (tableSource != null) {
      for (TableAccessPath path : tableSource.getTableAccessPaths()) {
        if (path.isTablePath()) {
          updateTablePath(session, tableSource, path);
        } else {
          updateIndexPath(session, tableSource, path);
        }
      }
    }
  }
}
