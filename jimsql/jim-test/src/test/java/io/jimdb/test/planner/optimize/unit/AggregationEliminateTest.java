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

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.sql.Planner;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class AggregationEliminateTest extends TestBase {

  protected static Planner planner;
  protected static Session session;

  private void run(Checker checker) {
    try {
      RelOperator relOperator = buildPlanAndOptimize(checker.getSql());
      Assert.assertNotNull(relOperator);
      checker.doCheck(relOperator);
    } catch (JimException e) {
      e.printStackTrace();
      Assert.assertTrue(checker.isHasException());
    }
  }


  @Test
  public void testCount() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select count(name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testSum() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select sum(name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testAvg() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select avg(name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }
  @Test
  public void testMulti() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select count(name) , avg(name) , sum(name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testCountDistinct() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select count(distinct name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testSumDistinct() {
    // name is primary key
    Checker checker = Checker.build()
            .sql("select sum(distinct name) from user group by name")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }
}
