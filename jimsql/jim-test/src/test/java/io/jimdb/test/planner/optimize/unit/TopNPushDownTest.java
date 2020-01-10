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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * TopNPushDownTest
 *
 * @since 2019-08-21
 */
public class TopNPushDownTest extends TestBase {

  private List<Checker> buildTestCases() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(Checker.build()
            .sql("select * from user where age = 10 limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,10)) -> Limit "
                    + "-> Projection"));

    checkerList.add(Checker.build()
            .sql("select name, count(age) from user limit 5")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(COUNT(test.user.age),DISTINCT(test.user.name),DISTINCT(test"
                            + ".user.age),DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Limit -> "
                            + "Projection"));

    checkerList.add(Checker.build()
            .sql("select name, count(age) from user order by score limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(COUNT(test.user.age),DISTINCT(test"
                    + ".user.name),DISTINCT(test.user.age),DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> "
                    + "TopN(Exprs=(test.user.score,ASC),Offset=0,Count=5) -> Projection -> Projection"));

    checkerList.add(Checker.build()
            .sql("select * from user where age = 10 order by name desc limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,10)) -> TopN"
                    + "(Exprs=(test.user.name,DESC),Offset=0,Count=5) -> Projection"));

    checkerList.add(Checker.build()
            .sql("select * from user where age = 10 order by name limit 1,5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,10)) -> TopN"
                    + "(Exprs=(test.user.name,ASC),Offset=1,Count=5) -> Projection"));

    checkerList.add(Checker.build()
            .sql("select * from user order by user.name limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> TopN(Exprs=(test.user.name,ASC),Offset=0,"
                    + "Count=5) -> Projection"));

    return checkerList;
  }

  private List<Checker> buildDebugTestCases() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(Checker.build()
            .sql("select * from user order by user.name limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> TopN(Exprs=(test.user.name,ASC),Offset=0,"
                    + "Count=5) -> Projection"));
    return checkerList;
  }

  @Test
  public void testTopNPushDown() {
//    List<Checker> checkerList = buildDebugTestCases();

    List<Checker> checkerList = buildTestCases();

    checkerList.forEach(checker -> {
      RelOperator logicalPlan = buildPlanTree(checker.getSql());
      logicalPlan = LogicalOptimizer.optimize(logicalPlan, OptimizeFlag.PUSHDOWNTOPN, session);
      String tree = OperatorUtil.printRelOperatorTree(logicalPlan);
      Assert.assertEquals("[" + checker.getSql() + "] not eq.", checker.getCheckPoints().get(CheckPoint.PlanTree), tree);
      System.out.println("[" + checker.getSql() + "]  after topN: \n" + tree + "\n");
    });
  }
}
