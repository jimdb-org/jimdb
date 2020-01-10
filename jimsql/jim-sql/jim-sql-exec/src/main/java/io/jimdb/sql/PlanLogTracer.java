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
package io.jimdb.sql;

import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "CLI_CONSTANT_LIST_INDEX" })
public class PlanLogTracer {
  private static final Logger LOG = LoggerFactory.getLogger(PlanLogTracer.class);
  private static boolean logicCloseLog = true;
  private static boolean physicalCloseLog = true;

  public static void sqlLogicPlanLOG(RelOperator logicalPlan, int optimizationFlag, boolean optimized) {
    if (logicCloseLog) {
      return;
    }

    // for getting less log, add some filters
    if (logicalPlan.getOperatorType() != Operator.OperatorType.LOGIC) {
      return;
    }

    StringBuilder builder = new StringBuilder(optimized ? "optimized logic-plan: " : "logic-plan: ");
    builder.append("optimizationFlag:").append(optimizationFlag).append(", tree: ")
            .append(OperatorUtil.printRelOperatorTree(logicalPlan, true));
    LOG.error(builder.toString());
  }

  public static void sqlPhysicalPlanLOG(SQLStatement stmt, Operator optimizedPlan) {
    if (physicalCloseLog) {
      return;
    }

    // for getting less log, add some filters
    if (optimizedPlan.getOperatorType() != Operator.OperatorType.LOGIC) {
      return;
    }

    String sql = stmt.toLowerCaseString();
    if (!sql.contains("select") || !sql.contains("from")) {
      return;
    }

    StringBuilder builder = new StringBuilder();
    builder.append(" sql: ").append(sql)
            .append(", physical-plan:").append(OperatorUtil.printRelOperatorTree(optimizedPlan, true))
            .append(", ").append(OperatorUtil.printPushDownedProcessors(optimizedPlan));
    LOG.error(builder.toString());
  }

}
