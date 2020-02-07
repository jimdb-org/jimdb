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
package io.jimdb.test.planner.optimize.unit;

import io.jimdb.core.values.BinaryValue;
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
public class AggregationTest extends TestBase {

  protected static Planner planner;
  protected static Session session;

  private void run(TestBase.Checker checker) {
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
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select count(*) from user group by name")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testDistinctValue() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select name,age from user group by name,age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> Projection");

    run(checker);
  }

  @Test
  public void testDistinctValue2() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select count(*),age from user group by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> HashAgg(COUNT(col_0),DISTINCT(col_1))");

    run(checker);
  }

  @Test
  public void testSum() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select sum(age) from user group by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> HashAgg(SUM(col_0))");

    run(checker);
  }

  @Test
  public void testSumIndex() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select sum(age) from user where age > 1  group by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "IndexSource(user) -> HashAgg(SUM(col_0))");

    run(checker);
  }

  @Test
  public void testSumIndex2() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select sum(age),phone from user where age > 1  group by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "IndexSource(user) -> HashAgg(SUM(col_0),DISTINCT(col_1))");

    run(checker);
  }

  /**
   * 2 check point:
   * 1. Check the ranges if is correct when a sql has a lot of 'or' expression.
   * 2. Or expression's value convert to bytes array, one byte value of the bytes array is 127 ,
   *    because java byte is signed , so 127 +1  = -128 , in this situation ,
   *    {@link BinaryValue#prefixNext}  maybe is wrong .
   */
  @Test
  public void testRange() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select age from user where age=1151 or age=1151100 or age=1151200 or age=1151300 or age=1151400 or "
                    + "age=1151500 or age=1151600 or age=1151700 or age=1151800 or age=1151900 or age=11511000 or "
                    + "age=11511100 or age=11511200 or age=11511300 or age=11511400 or age=11511500 or age=11511600 "
                    + "or age=11511700 or age=11511800 or age=11511900 or age=11512000 or age=11512100 or "
                    + "age=11512200 or age=11512300 or age=11512400 or age=11512500 or age=11512600 or age=11512700 "
                    + "or age=11512800 or age=11512900 or age=11513000 or age=11513100 or age=11513200 or "
                    + "age=11513300 or age=11513400 or age=11513500 or age=11513600 or age=11513700 or age=11513800 "
                    + "or age=11513900 or age=11514000")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:11511000,end:11511000}],startInclusive:true,endInclusive:true},{[{start:11511200,end:11511200}],startInclusive:true,endInclusive:true},{[{start:11511100,end:11511100}],startInclusive:true,endInclusive:true},{[{start:11511500,end:11511500}],startInclusive:true,endInclusive:true},{[{start:11511300,end:11511300}],startInclusive:true,endInclusive:true},{[{start:11511400,end:11511400}],startInclusive:true,endInclusive:true},{[{start:11511700,end:11511700}],startInclusive:true,endInclusive:true},{[{start:11511800,end:11511800}],startInclusive:true,endInclusive:true},{[{start:11511600,end:11511600}],startInclusive:true,endInclusive:true},{[{start:11512000,end:11512000}],startInclusive:true,endInclusive:true},{[{start:11511900,end:11511900}],startInclusive:true,endInclusive:true},{[{start:11512200,end:11512200}],startInclusive:true,endInclusive:true},{[{start:11512300,end:11512300}],startInclusive:true,endInclusive:true},{[{start:11512100,end:11512100}],startInclusive:true,endInclusive:true},{[{start:11512500,end:11512500}],startInclusive:true,endInclusive:true},{[{start:11512400,end:11512400}],startInclusive:true,endInclusive:true},{[{start:11512800,end:11512800}],startInclusive:true,endInclusive:true},{[{start:11512600,end:11512600}],startInclusive:true,endInclusive:true},{[{start:11512700,end:11512700}],startInclusive:true,endInclusive:true},{[{start:11513000,end:11513000}],startInclusive:true,endInclusive:true},{[{start:11512900,end:11512900}],startInclusive:true,endInclusive:true},{[{start:11513300,end:11513300}],startInclusive:true,endInclusive:true},{[{start:11513100,end:11513100}],startInclusive:true,endInclusive:true},{[{start:11513200,end:11513200}],startInclusive:true,endInclusive:true},{[{start:11513500,end:11513500}],startInclusive:true,endInclusive:true},{[{start:11513400,end:11513400}],startInclusive:true,endInclusive:true},{[{start:11513800,end:11513800}],startInclusive:true,endInclusive:true},{[{start:11513600,end:11513600}],startInclusive:true,endInclusive:true},{[{start:11513700,end:11513700}],startInclusive:true,endInclusive:true},{[{start:11514000,end:11514000}],startInclusive:true,endInclusive:true},{[{start:11513900,end:11513900}],startInclusive:true,endInclusive:true},{[{start:1151,end:1151}],startInclusive:true,endInclusive:true},{[{start:1151200,end:1151200}],startInclusive:true,endInclusive:true},{[{start:1151100,end:1151100}],startInclusive:true,endInclusive:true},{[{start:1151400,end:1151400}],startInclusive:true,endInclusive:true},{[{start:1151300,end:1151300}],startInclusive:true,endInclusive:true},{[{start:1151700,end:1151700}],startInclusive:true,endInclusive:true},{[{start:1151500,end:1151500}],startInclusive:true,endInclusive:true},{[{start:1151600,end:1151600}],startInclusive:true,endInclusive:true},{[{start:1151900,end:1151900}],startInclusive:true,endInclusive:true},{[{start:1151800,end:1151800}],startInclusive:true,endInclusive:true}]");

    run(checker);
  }

  @Test
  public void testSum2() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select sum(age) from user where age > 1  group by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "IndexSource(user) -> HashAgg(SUM(col_0))");

    run(checker);
  }
  @Test
  public void testDistinct() {
    TestBase.Checker checker = TestBase.Checker.build()
            .sql("select DISTINCT(age) from user order by age")
            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> HashAgg(DISTINCT(col_0)) -> Order");

    run(checker);
  }
}
