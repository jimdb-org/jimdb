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

package io.jimdb.test.planner.optimize.unit;

import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.test.planner.TestBase;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizeFlag;

import org.junit.Assert;
import org.junit.Test;

public class PredPushDownTest extends TestBase {

  @Test
  public void testPredicatePushDown_simple() {
    // sql input and expected result
    String sql = "select name from user where score=20";
    String tableSourceStr = "TableSource={columns=[name,age,phone,score,salary], "
            + "pushDownPredicates=[EqualInt(test.user.score,20)]}";

    // build logical plan tree
    RelOperator logicalPlan = buildPlanTree(sql);
    Assert.assertNotNull(logicalPlan);

    // do optimization
    logicalPlan = doOptimize(logicalPlan);
    System.out.println(OperatorUtil.printRelOperatorTree(logicalPlan));
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];
    System.out.println(tableSource.toString());

    // check result
    Assert.assertEquals(1, tableSource.getPushDownPredicates().size());
    Assert.assertEquals(tableSourceStr, tableSource.toString());
  }

  @Test
  public void testPredicatePushDown_cond_and() {
    // sql input and expected result
    String sql = "select name from user where score=20 and age=30";
    String tableSourceStr = "TableSource={columns=[name,age,phone,score,salary], "
            + "pushDownPredicates=[EqualInt(test.user.score,20),EqualInt(test.user.age,30)]}";

    // build logical plan tree
    RelOperator logicalPlan = buildPlanTree(sql);
    Assert.assertNotNull(logicalPlan);

    // do optimization
    logicalPlan = doOptimize(logicalPlan);
    System.out.println(OperatorUtil.printRelOperatorTree(logicalPlan));
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];
    System.out.println(tableSource.toString());

    // check result
    Assert.assertEquals(2, tableSource.getPushDownPredicates().size());
    Assert.assertEquals(tableSourceStr, tableSource.toString());
  }

  @Test
  public void testPredicatePushDown_cond_or() {
    // sql input and expected result
    String sql = "select name from user where score=20 or age=30";
    String tableSourceStr = "TableSource={columns=[name,age,phone,score,salary], "
            + "pushDownPredicates=[LogicOr(EqualInt(test.user.score,20),EqualInt(test.user.age,30))]}";

    // build logical plan tree
    RelOperator logicalPlan = buildPlanTree(sql);
    Assert.assertNotNull(logicalPlan);

    // do optimization
    logicalPlan = doOptimize(logicalPlan);
    System.out.println(OperatorUtil.printRelOperatorTree(logicalPlan));
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0];
    System.out.println(tableSource.toString());

    // check result
    Assert.assertEquals(1, tableSource.getPushDownPredicates().size());
    Assert.assertEquals(tableSourceStr, tableSource.toString());
  }

  @Test
  public void testPredicatePushDown_agg_func() {
    // sql input and expected result
    String sql = "select count(*) from user";
    String tableSourceStr = "TableSource={columns=[name,age,phone,score,salary]}";

    // build logical plan tree
    RelOperator logicalPlan = buildPlanTree(sql);
    Assert.assertNotNull(logicalPlan);

    // do optimization
    logicalPlan = doOptimize(logicalPlan);
    System.out.println(OperatorUtil.printRelOperatorTree(logicalPlan));
    TableSource tableSource = (TableSource) logicalPlan.getChildren()[0].getChildren()[0];
    System.out.println(tableSource.toString());

    // check result: aggregation function is not pushed down in logical optimization
    Assert.assertEquals(0, tableSource.getPushDownPredicates().size());
    Assert.assertEquals(tableSourceStr, tableSource.toString());
  }

  private RelOperator doOptimize(RelOperator logicalPlan) {
    return LogicalOptimizer.optimize(logicalPlan, OptimizeFlag.PREDICATEPUSHDOWN, session);
  }
}
