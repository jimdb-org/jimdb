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
package io.jimdb.test.planner.optimize.integration;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.JimException;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * LogicalOptimizationTest
 *
 * @since 2019/8/23
 */
public class LogicalOptimizationTest extends TestBase {

  @Test
  public void testLogicalOptimization() {

    List<Checker> checkerList = buildAllTestCases();
    checkerList.forEach(checker -> {
      try {
        RelOperator relOperator = buildPlanAndLogicalOptimizeOnly(checker.getSql());
        Assert.assertNotNull(relOperator);

        checker.doCheck(relOperator);
      } catch (JimException e) {
        Assert.assertTrue(checker.isHasException());
        Assert.assertEquals(checker.getCheckPoints().get(CheckPoint.PlanTree), e.toString());
      }
    });
  }

  @Test
  public void testComposite_Case1() {
    Checker checker = Checker.build()
            .sql("select name from user where name='Tom' and age=10")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection")
            .addCheckPoint(CheckPoint.PredicatesPushDown,
                    "TableSource.pushDownPredicates=[FUNC-EqualString(name,Tom),FUNC-EqualInt(age,10)]")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name,age]")
            .hasException(true);

    try {
      RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
      Assert.assertNotNull(planTree);
      checker.doCheck(planTree);
    } catch (JimException e) {
      Assert.assertTrue(checker.isHasException());
    }
  }

  @Test
  public void testComposite_Case3() {
    Checker checker = Checker.build()
            .sql("select phone from user where score=80 order by name")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> Order -> Projection")
            .addCheckPoint(CheckPoint.PredicatesPushDown,
                    "TableSource.pushDownPredicates=[FUNC-EqualInt(score,80)]")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name,phone,score]");

    try {
      RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
      Assert.assertNotNull(planTree);
      checker.doCheck(planTree);
    } catch (JimException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testComposite_Case4() {
    Checker checker = Checker.build()
            .sql("select name as username, count(age) from user order by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> "
                            + "HashAgg(COUNT(test.user.age),DISTINCT(test.user.name),DISTINCT(test.user.score)) -> "
                            + "Order -> Projection")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name,age,score]")
            .addCheckPoint(CheckPoint.BuildKeyInfo, "TableSource.schema.keyColumns=[name]");

    try {
      RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
      Assert.assertNotNull(planTree);
      checker.doCheck(planTree);
    } catch (JimException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testComposite_Case5() {
    Checker checker = Checker.build()
            .sql("select sum(1) from user where name='Tom'")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(SUM(1)) -> Projection")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name]")
            .addCheckPoint(CheckPoint.BuildKeyInfo, "TableSource.schema.keyColumns=[name]")
            .addCheckPoint(CheckPoint.PredicatesPushDown,
                    "TableSource.pushDownPredicates=[FUNC-EqualString(name,Tom)]");

    try {
      RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
      Assert.assertNotNull(planTree);
      checker.doCheck(planTree);
    } catch (JimException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testComposite_Case6() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(Checker.build()
            .sql("select name from user where age=score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> Projection")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name,age,score]")
            .addCheckPoint(CheckPoint.PredicatesPushDown,
                    "TableSource.pushDownPredicates=[FUNC-EqualInt(age,score)]"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> Projection")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name]")
            .addCheckPoint(CheckPoint.PredicatesPushDown, "TableSource.pushDownPredicates=[]"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1 and age=10")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> Projection")
            .addCheckPoint(CheckPoint.ColumnPrune,
                    "TableSource.columns=[name,age]")
            .addCheckPoint(CheckPoint.PredicatesPushDown,
                    "TableSource.pushDownPredicates=[FUNC-EqualInt(age,10)]"));

    try {
      checkerList.forEach(checker -> {
        RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
        Assert.assertNotNull(planTree);
        checker.doCheck(planTree);
      });
    } catch (JimException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testComposite_Case7() {
    Checker checker = Checker.build()
            .sql("select phone,count(age),count(name)+sum(age),sum(age+score) from user u " +
                    "where score=age+10 and (name='Tom' or age=10) order by sum(score)+1 limit 1,5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> HashAgg(COUNT(test.u.age)," +
                    "COUNT(test.u.name),SUM(test.u.age),SUM(PlusInt(test.u.age,test.u.score)),SUM(test.u.score)," +
                    "DISTINCT(test.u.phone)) -> TopN(Exprs=(PlusInt(agg_col_4,1),ASC),Offset=1,Count=5) -> " +
                    "Projection -> Projection");

    try {
      RelOperator planTree = buildPlanAndLogicalOptimizeOnly(checker.getSql());
      Assert.assertNotNull(planTree);
      checker.doCheck(planTree);
    } catch (JimException e) {
      Assert.fail(e.getMessage());
    }
  }

  private List<Checker> buildAllTestCases() {
    List<Checker> checkerList = new ArrayList<>();

    // dual table
    checkerList.add(Checker.build()
            .sql("select 1")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1+1")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1+1 from dual")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));

    // TableSource
    checkerList.add(Checker.build()
            .sql("select name from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1+1 from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select user.* from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));

    // selection
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 and score=100")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where name ='Tom' and (age=1 or score=100)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where age=score")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1 and age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));

    // aggregation
    checkerList.add(Checker.build()
            .sql("select sum(1) from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(SUM(1)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select sum(1) from user where name='Tom'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(SUM(1)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age,count(*) from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "HashAgg(COUNT(1),DISTINCT(test.user.name),DISTINCT(test.user.age)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select count(1)+sum(age)+sum(score) from user where score=100 and (name='Tom' or age=10)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "HashAgg(COUNT(1),SUM(test.user.age),SUM(test.user.score)) -> Projection"));

    // order by
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by age desc,name desc")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name from user order by count(name)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "HashAgg(COUNT(test.user.name),DISTINCT(test.user.name)) -> Order -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by (age+score)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Order -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,count(score) from user where score=100 and (name='Tom' or age=10) order by sum(name)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "HashAgg(COUNT(test.user.score),SUM(test.user.name),DISTINCT(test.user.name)) -> Order -> Projection"));

    // limit
    checkerList.add(Checker.build()
            .sql("select name from user limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Limit -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by age desc limit 1,5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "TopN(Exprs=(test.user.age,DESC),Offset=1,Count=5) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where age=1 order by score limit 10")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "TopN(Exprs=(test.user.score,ASC),Offset=0,Count=10) -> Projection"));

    // distinct
    checkerList.add(Checker.build()
            .sql("select distinct name from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(DISTINCT(test.user.name))"));
    checkerList.add(Checker.build()
            .sql("select distinct name from user where age = 10 limit 10")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "HashAgg(DISTINCT(test.user.name)) -> Limit"));

    // topN
    checkerList.add(Checker.build()
            .sql("select * from user order by user.name limit 5")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> TopN(Exprs=(test.user.name,ASC),Offset=0,Count=5) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name as c1, age as c2 from user order by 1, score+score limit 2")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> TopN(Exprs=((test.user.name,ASC)(PlusInt(test.user.score,test.user.score),"
                            + "ASC)),Offset=0,Count=2) -> Projection"));

    checkerList.add(Checker.build()
            .sql("select phone,count(age),count(name)+sum(age),sum(age+score) from user u " +
                    "where score=age+10 and (name='Tom' or age=10) order by sum(score)+1 limit 1,5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> HashAgg(COUNT(test.u.age)," +
                    "COUNT(test.u.name),SUM(test.u.age),SUM(PlusInt(test.u.age,test.u.score)),SUM(test.u.score)," +
                    "DISTINCT(test.u.phone)) -> TopN(Exprs=(PlusInt(agg_col_4,1),ASC),Offset=1,Count=5) -> " +
                    "Projection -> Projection"));

    return checkerList;
  }
}
