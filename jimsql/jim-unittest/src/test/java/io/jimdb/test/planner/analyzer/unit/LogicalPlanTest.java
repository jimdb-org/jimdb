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
package io.jimdb.test.planner.analyzer.unit;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner Tester.
 */
public class LogicalPlanTest extends TestBase {
  private static Logger LOG = LoggerFactory.getLogger(LogicalPlanTest.class);

  @Test
  public void testLogicalPlan() {
    List<Checker> checkerList = builderTestCase();
    checkerList.forEach(checker -> {
      try {
        RelOperator relOperator = buildPlanTree(checker.getSql());
        Assert.assertNotNull(relOperator);

        String tree = OperatorUtil.printRelOperatorTree(relOperator);
        LOG.debug("The original SQL is ({})", checker.getSql());
        LOG.debug("The logical tree is ({})", tree);
        checker.doCheck(relOperator);
      } catch (BaseException e) {
        LOG.error("cause error, checker sql {}", checker.getSql(), e);
        Assert.assertTrue(checker.isHasException());
        Assert.assertEquals(checker.getCheckPoints().get(CheckPoint.PlanTree), e.toString());
      }
    });
  }

  private List<Checker> buildDebugTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(Checker.build()
            .sql("select name,age, score from user where age=1 order by 3")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order"));
    return checkerList;
  }

  @Override
  protected List<Checker> builderTestCase() {
    List<Checker> checkerList = new ArrayList<>();

    // dual
    checkerList.add(Checker.build()
            .sql("select 1")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1+1")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1+1 from dual")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));

    // tableSource
    checkerList.add(Checker.build()
            .sql("select * from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));

    // alias
    checkerList.add(Checker.build()
            .sql("select u.* from user u")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select u.* from user as u")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> Projection"));

    // selection
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 and score=100")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1),EqualInt"
                    + "(test.user.score,100)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 or score=100")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(LogicOr(EqualInt(test.user.age,1),"
                    + "EqualInt(test.user.score,100))) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where age=score")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> " +
                    "Selection(EqualInt(test.user.age,test.user.score)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user where 1=1 and age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where name ='Tom' and (age =1 or score =100)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualString(test.user.name,Tom),"
                    + "LogicOr(EqualInt(test.user.age,1),EqualInt(test.user.score,100))) -> Projection"));

    // aggregation
    checkerList.add(Checker.build()
            .sql("select sum(1) from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(SUM(1),DISTINCT(test.user.name),"
                    + "DISTINCT(test.user.age),DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user"
                    + ".salary)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select sum(1) from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "HashAgg(SUM(1),DISTINCT(test.user.name),DISTINCT(test.user.age),DISTINCT(test.user.phone),"
                    + "DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age,sum(1) from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "HashAgg(SUM(1),DISTINCT(test.user.name),DISTINCT(test.user.age),DISTINCT(test.user.phone),"
                    + "DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,age,count(*) from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "HashAgg(COUNT(1),DISTINCT(test.user.name),DISTINCT(test.user.age),DISTINCT(test.user"
                    + ".phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select count(1+1) from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(COUNT(2),DISTINCT(test.user.name),"
                    + "DISTINCT(test.user.age),DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user"
                    + ".salary)) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select count(1)+sum(age)+sum(score) from user where score=100 and (name='Tom' or age=10)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.score,100)," +
                    "LogicOr(EqualString(test.user.name,Tom),EqualInt(test.user.age,10))) -> HashAgg(COUNT(1)," +
                    "SUM(test.user.age),SUM(test.user.score),DISTINCT(test.user.name),DISTINCT(test.user.age)," +
                    "DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection"));

    // projection
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(Checker.build()
            .sql("select name as c1,age a from user where age=1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(Checker.build()
            .sql("select *,1 from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select 1, user.* from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));

    // order by
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by age desc")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by age desc,name desc")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by 1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by 1+1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by (age+score)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name from user order by count(name)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> HashAgg(COUNT(test.user.name),DISTINCT("
                    + "test.user.name),DISTINCT(test.user.age),DISTINCT(test.user.phone),DISTINCT(test.user.score),"
                    + "DISTINCT(test.user.salary)) -> Projection -> Order -> Projection"));
    checkerList.add(Checker.build()
            .sql(" select name as ff from user where age=1 order by count(ff) desc")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "HashAgg(COUNT(test.user.name),DISTINCT(test.user.name),DISTINCT(test.user.age),DISTINCT"
                    + "(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection -> Order"
                    + " -> Projection"));
    checkerList.add(Checker.build()
            .sql("select name,count(score) from user where score=100 and (name='Tom' or age=10) order by sum(name)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.score,100),LogicOr"
                    + "(EqualString(test.user.name,Tom),EqualInt(test.user.age,10))) -> HashAgg(COUNT(test.user"
                    + ".score),SUM(test.user.name),DISTINCT(test.user.name),DISTINCT(test.user.age),DISTINCT(test"
                    + ".user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection -> Order -> "
                    + "Projection"));
    checkerList.add(Checker.build()
            .sql("select name as name1 from user order by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name as name1 from user order by user.name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection -> Order"));
    checkerList.add(Checker.build()
            .sql("select name as name1 from user u order by u.name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> Projection -> Order"));

    // limit
    checkerList.add(Checker.build()
            .sql("select name from user limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection -> Limit"));
    checkerList.add(Checker.build()
            .sql("select name,age from user where age=1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order -> Limit"));
    checkerList.add(Checker.build()
            .sql("select name from user where age=1 order by score limit 10")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,1)) -> "
                    + "Projection -> Order -> Limit -> Projection"));

    // KeyGet
    checkerList.add(Checker.build()
            .sql("select name from user where age=18 and phone='123'")
            .addCheckPoint(CheckPoint.PlanTree, "KeyGet((idx_age_phone(age,phone),values(18:LONG,123:STRING))) -> "
                    + "Projection(test.user.name)"));
    checkerList.add(Checker.build()
            .sql("select age from user where name='Tom' or name='Jack'")
            .addCheckPoint(CheckPoint.PlanTree, "KeyGet((primary(name),values(Tom:STRING)),(primary(name),values(Jack:STRING)))"
                    + " -> Projection(test.user.age)"));

    // use/force index
    checkerList.add(Checker.build()
            .sql("select age from user use index(idx_age_phone)")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection"));
    checkerList.add(Checker.build()
            .sql("select age from user use index(idx_age) force index(idx_age_x)")
            .addCheckPoint(CheckPoint.PlanTree, DBException.get(ErrorModule.META,
                    ErrorCode.ER_KEY_COLUMN_DOES_NOT_EXISTS, "idx_age_x").toString())
            .hasException(true));

    // distinct
    checkerList.add(Checker.build()
            .sql("select distinct name from user")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection -> HashAgg(DISTINCT(test.user"
                    + ".name))"));
    checkerList.add(Checker.build()
            .sql("select distinct name from user where age = 10 limit 10")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.age,10)) -> "
                    + "Projection -> HashAgg(DISTINCT(test.user.name)) -> Limit"));

    // compositive
    checkerList.add(Checker.build()
            .sql("select count(1)+sum(age) from user where score=100 and (name='Tom' or age=10) order by sum(score)+1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(EqualInt(test.user.score,100),"
                    + "LogicOr(EqualString(test.user.name,Tom),EqualInt(test.user.age,10))) -> HashAgg(COUNT(1),"
                    + "SUM(test.user.age),SUM(test.user.score),DISTINCT(test.user.name),DISTINCT(test.user.age),"
                    + "DISTINCT(test.user.phone),DISTINCT(test.user.score),DISTINCT(test.user.salary)) -> Projection "
                    + "-> Order -> Projection"));
    checkerList.add(Checker.build()
            .sql("select phone,count(age),count(name)+sum(age),sum(age+score) from user u " +
                    "where score=age+10 and (name='Tom' or age=10) order by sum(score)+1 limit 1,5")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u) -> Selection(EqualInt(test.u.score," +
                    "PlusInt(test.u.age,10)),LogicOr(EqualString(test.u.name,Tom),EqualInt(test.u.age,10))) -> " +
                    "HashAgg(COUNT(test.u.age),COUNT(test.u.name),SUM(test.u.age),SUM(PlusInt(test.u.age,test.u"
                    + ".score))," +
                    "SUM(test.u.score),DISTINCT(test.u.name),DISTINCT(test.u.age),DISTINCT(test.u.phone)," +
                    "DISTINCT(test.u.score),DISTINCT(test.u.salary)) -> Projection -> Order -> Limit -> Projection"));
    return checkerList;
  }
}
