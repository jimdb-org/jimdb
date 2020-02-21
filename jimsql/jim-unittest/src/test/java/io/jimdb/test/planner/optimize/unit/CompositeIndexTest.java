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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.jimdb.common.exception.JimException;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.physical.RangeRebuildVisitor;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.mock.meta.MockMeta;
import io.jimdb.test.mock.meta.MockMetaStore4CompositeIndex;
import io.jimdb.test.planner.TestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * @version V1.0
 */
public class CompositeIndexTest extends TestBase {

  private Logger logger = LoggerFactory.getLogger(CompositeIndexTest.class);

  private Checker checker = Checker.build(new CheckMethod());
  private RangeRebuildChecker rangeRebuildChecker = RangeRebuildChecker.build(new RangeRebuildCheckMethod());
  private static MockMeta mockMeta = new MockMetaStore4CompositeIndex();

  @BeforeClass
  public static void mock() throws Exception {
//    mock tables
    setMockMeta(mockMeta);
    setup();
  }

  @After
  public void reset() {
    checker.reset();
    rangeRebuildChecker.reset();
    TableStatsManager.resetAllTableStats();
  }

  @Test
  public void testFullRange() {
    checker.sql("select name from user")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:MIN_VALUE,end:MAX_VALUE}],startInclusive:true,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user)")
            .check();
  }

  @Test
  public void testNotIndex() {
    checker.sql("select name from user where age > 10 or age < 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:MIN_VALUE,end:MAX_VALUE}],startInclusive:true,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Projection")
            .check();
  }

  // region normal composite index

  @Test
  public void testOr() {
    checker.sql("select name from user where name = 'tom' or name = 'jerry' ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:jerry,end:jerry}],startInclusive:true,"
                    + "endInclusive:true},{[{start:tom,end:tom}],startInclusive:true,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), EqualString(test.user.name,jerry)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user)")
            .check();
  }

  @Test
  public void testOr2() {
    checker.sql("select name from user where name = 'tom' or ( name = 'jerry' and age > 1 ) ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:jerry,end:jerry},{start:1,end:MAX_VALUE}],"
                    + "startInclusive:false,endInclusive:true},{[{start:tom,end:tom}],startInclusive:true,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), EqualString(test.user.name,jerry), GreaterInt(test.user.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> Projection")
            .check();
  }

  @Test
  public void testOr3() {
    checker.sql("select name from user where name > 'a' and name < 'd' or ( name > 'b' and name < 'c' ) ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:a,end:d}],startInclusive:false,endInclusive:false}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[GreaterString(test"
                    + ".user.name,a), LessString(test.user.name,d), GreaterString(test.user.name,b), LessString(test"
                    + ".user.name,c)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user)")
            .check();
  }

  @Test
  public void testOr4() {
    checker.sql("select name from user where name > 'a' and name < 'd' or ( name > 'b' and name < 'f' ) ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:a,end:f}],startInclusive:false,endInclusive:false}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[GreaterString(test"
                    + ".user.name,a), LessString(test.user.name,d), GreaterString(test.user.name,b), LessString(test"
                    + ".user.name,f)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user)")
            .check();
  }

  @Test
  public void testIndexSource() {
    checker.sql("select name from user where  name = 'tom' and age > 10 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:MAX_VALUE}],"
                    + "startInclusive:false,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), GreaterInt(test.user.age,10)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> Projection")
            .check();
  }

  @Test
  public void testIndexLookup() {
    checker.sql("select * from user where  name = 'tom' and age = 10 and  phone = '1212313' and score >1.11 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:1.11,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), EqualInt(test.user.age,10), EqualString(test.user.phone,1212313), GreaterReal"
                    + "(test.user.score,1.11)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> TableSource(user) -> IndexLookUp")
            .check();
  }

  @Test
  public void testCompositeIndexSource() {
    checker.sql("select * from user where  name = 'tom' and age = 10 and  phone = '1212313' and score  = 99.99 and "
            + "salary > 1000.00 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:99.99,end:99.99},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), EqualInt(test.user.age,10), EqualString(test.user.phone,1212313), EqualReal"
                    + "(test.user.score,99.99), GreaterDecimal(test.user.salary,1000.00)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> TableSource(user) -> IndexLookUp")
            .check();
  }

  @Test
  public void testCompositeIndexIndexLookup() {
    checker.sql("select * from user where  name = 'tom' and age = 10 and  phone = '1212313' and score  = 99 and "
            + "remark = 'remark' and salary > 1000.00 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:99.0,end:99.0},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user.name,tom), EqualInt(test.user.age,10), EqualString(test.user.phone,1212313), EqualReal"
                    + "(test.user.score,99.0), GreaterDecimal(test.user.salary,1000.00)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user) -> TableSource(user) -> Selection(EqualString(test"
                    + ".user.remark,remark)) -> IndexLookUp")
            .check();
  }

  @Test
  public void testNotIndexUnique() {
    checker.sql("select name from user_unique_idx where age > 10 or age < 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:MIN_VALUE,end:MAX_VALUE}],startInclusive:true,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user_unique_idx) -> Projection")
            .check();
  }

  // endregion

  // region unique composite index

  @Test
  public void testIndexSourceUnique() {
    checker.sql("select name from user_unique_idx where  name = 'tom' and age > 10 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:MAX_VALUE}],"
                    + "startInclusive:false,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user_unique_idx.name,tom), GreaterInt(test.user_unique_idx.age,10)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_unique_idx) -> Projection")
            .check();
  }

  @Test
  public void testIndexLookupUnique() {
    checker.sql("select * from user_unique_idx where  name = 'tom' and age = 10 and  phone = '1212313' and score >1"
            + ".11 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:1.11,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user_unique_idx.name,tom), EqualInt(test.user_unique_idx.age,10), EqualString(test"
                    + ".user_unique_idx.phone,1212313), GreaterReal(test.user_unique_idx.score,1.11)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_unique_idx) -> TableSource(user_unique_idx) -> "
                    + "IndexLookUp")
            .check();
  }

  @Test
  public void testCompositeIndexSourceUnique() {
    checker.sql("select * from user_unique_idx where  name = 'tom' and age = 10 and  phone = '1212313' and score  = "
            + "99.99 and salary > 1000.00 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:99.99,end:99.99},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user_unique_idx.name,tom), EqualInt(test.user_unique_idx.age,10), EqualString(test"
                    + ".user_unique_idx.phone,1212313), EqualReal(test.user_unique_idx.score,99.99), GreaterDecimal"
                    + "(test.user_unique_idx.salary,1000.00)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_unique_idx) -> TableSource(user_unique_idx) -> "
                    + "IndexLookUp")
            .check();
  }

  @Test
  public void testCompositeIndexIndexLookupUnique() {
    checker.sql("select * from user_unique_idx where  name = 'tom' and age = 10 and  phone = '1212313' and score  = "
            + "99 and remark = 'remark' and salary > 1000.00 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:99.0,end:99.0},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
                    + "endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"[EqualString(test"
                    + ".user_unique_idx.name,tom), EqualInt(test.user_unique_idx.age,10), EqualString(test"
                    + ".user_unique_idx.phone,1212313), EqualReal(test.user_unique_idx.score,99.0), GreaterDecimal"
                    + "(test.user_unique_idx.salary,1000.00)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_unique_idx) -> TableSource(user_unique_idx) -> "
                    + "Selection(EqualString(test.user_unique_idx.remark,remark)) -> IndexLookUp")
            .check();
  }

  @Test
  public void testDate() {
    checker.sql("select * from user_date_unique_idx where  name = 'tom' and age = 10 and  phone = '1212313' and "
            + " score = 99.0 and salary = 1000.00 and date = '2013-01-12' and time < '23:23:56' ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,end:1212313},{start:99.0,end:99.0},{start:1000.00,end:1000.00},{start:1841576423408533504,end:1841576423408533504},{start:MIN_VALUE,end:84236000000}],startInclusive:true,endInclusive:false}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"idx_name_age_phone_salary_date_time\":\"[EqualString(test"
                    + ".user_date_unique_idx.name,tom), EqualInt(test.user_date_unique_idx.age,10), EqualString(test"
                    + ".user_date_unique_idx.phone,1212313), EqualReal(test.user_date_unique_idx.score,99.0), "
                    + "EqualDecimal(test.user_date_unique_idx.salary,1000.00), EqualDate(test.user_date_unique_idx"
                    + ".date,1841576423408533504), LessTime(test.user_date_unique_idx.time,84236000000)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_date_unique_idx) -> TableSource"
                    + "(user_date_unique_idx) -> IndexLookUp")
            .check();
  }

  // endregion

  // region unique composite index  with date 、time

//  @Test
//  public void testIn() {
//    checker.sql("select * from user where name in ('tom','jerry') ")
//            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
//                    + "end:1212313},{start:99.0,end:99.0},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
//                    + "endInclusive:true}]")
//            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_name_age_phone_score_salary\":\"accessConditions"
//                    + "=[EqualString(test.user_unique_idx.name,tom), EqualInt(test.user_unique_idx.age,10), "
//                    + "EqualString(test.user_unique_idx.phone,1212313), EqualReal(test.user_unique_idx.score,99.0), "
//                    + "GreaterDecimal(test.user_unique_idx.salary,1000.00)]\"}")
//            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(user_unique_idx) -> TableSource(user_unique_idx) -> "
//                    + "Selection(EqualString(test.user_unique_idx.remark,remark)) -> IndexLookUp")
//            .check();
//  }

  // endregion


//
  @Test
  public void testFix() {
    checker.sql("select count(1) as countRecord, t_int, t_unique, t_index\n" +
            "        , t_tinyint, t_smallint, t_varchar\n" +
            "from fix\n" +
            "where t_unique = '8427'")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:8427,end:8427}],startInclusive:true,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_t_unique\":\"[EqualString(test.fix.t_unique,8427)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(fix) -> TableSource(fix) -> HashAgg(COUNT(1),DISTINCT(test.fix.t_int),DISTINCT(test.fix.t_unique),DISTINCT(test.fix.t_index),DISTINCT(test.fix.t_tinyint),DISTINCT(test.fix.t_smallint),DISTINCT(test.fix.t_varchar)) -> IndexLookUp -> HashAgg(COUNT(col_0),DISTINCT(col_1),DISTINCT(col_2),DISTINCT(col_3),DISTINCT(col_4),DISTINCT(col_5),DISTINCT(col_6))")
            .check();
  }

//  @Test
  public void testFixParallel() {

    ExecutorService executorService = Executors.newFixedThreadPool(200);

    for (int i = 0; i < 1000000; i++) {
      int finalI = i;
      executorService.submit(() -> {
        try {
          String sql = "select count(1) as countRecord, t_int, t_unique, t_index\n" +
                  "        , t_tinyint, t_smallint, t_varchar\n" +
                  "from fix\n" +
                  "where t_unique = '" + finalI + "' group by t_varchar;";
          Checker checker = Checker.build(new CheckMethod());
          checker.sql(sql).check();
        } catch (Exception e) {
          logger.error("testFixParallel-->", e);
        }
      });
    }
  }

//  @Test
  public void testFixParallel2() {
    Flux.range(1, 1000)
            .map(i -> "select count(1) as countRecord, t_int, t_unique, t_index\n" +
                    "        , t_tinyint, t_smallint, t_varchar\n" +
                    "from fix\n" +
                    "where t_unique = '" + i + "' group by t_varchar;")
            .parallel()
            .runOn(Schedulers.newParallel("test###", 10))
            .doOnComplete(() -> {
              logger.error("Terminate-------------");
            })
            .subscribe(sql -> {
              try {
                logger.error(Thread.currentThread().getName());
                Checker checker = Checker.build(new CheckMethod());
                checker.sql(sql).check();
              } catch (Exception e) {
                logger.error("testFixParallel-->", e);
                e.printStackTrace();
              }
            });

    try {
      Thread.sleep(10000000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFix2() {
    checker.sql("select min(t_float) as minFloat, max(t_double) as maxDouble\n"
            + "        , count(t_unique) as countUnique, sum(t_int) as sumInt\n"
            + "            , t_int, t_unique, t_index, t_tinyint, t_smallint\n"
            + "            , t_varchar, t_float, t_double\n"
            + "    from fix\n"
            + "    where t_unique = '108'\n"
            + "    group by t_tinyint")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:108,end:108}],startInclusive:true,endInclusive:true}]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"uni_t_unique\":\"[EqualString(test.fix.t_unique,108)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(fix) -> TableSource(fix) -> HashAgg(MIN(test.fix.t_float),MAX(test.fix.t_double),COUNT(test.fix.t_unique),SUM(test.fix.t_int),DISTINCT(test.fix.t_int),DISTINCT(test.fix.t_unique),DISTINCT(test.fix.t_index),DISTINCT(test.fix.t_tinyint),DISTINCT(test.fix.t_smallint),DISTINCT(test.fix.t_varchar),DISTINCT(test.fix.t_float),DISTINCT(test.fix.t_double)) -> IndexLookUp -> HashAgg(MIN(col_0),MAX(col_1),COUNT(col_2),SUM(col_3),DISTINCT(col_4),DISTINCT(col_5),DISTINCT(col_6),DISTINCT(col_7),DISTINCT(col_8),DISTINCT(col_9),DISTINCT(col_10),DISTINCT(col_11))")
            .check();
  }

//  public void testFix2() {
//
//    TestBase.Checker checker = TestBase.Checker.build()
//            .sql("select min(t_float) as minFloat, max(t_double) as maxDouble\n"
//                    + "        , count(t_unique) as countUnique, sum(t_int) as sumInt\n"
//                    + "            , t_int, t_unique, t_index, t_tinyint, t_smallint\n"
//                    + "            , t_varchar, t_float, t_double\n"
//                    + "    from fix\n"
//                    + "    where t_unique = '108'\n"
//                    + "    group by t_tinyint")
//            .addCheckPoint(TestBase.CheckPoint.PlanTree, "TableSource(user) -> HashAgg(COUNT(col_0))");
//
//    run(checker);
//  }

  // region range rebuild test


  @Test
  public void testRebuildUpdate() {
    rangeRebuildChecker.sql("update user_unique_idx set name = 10  where  name = 'tom' and age > 10 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]")
            .check();
  }

  //todo insert sql with where
  @Test
  public void testRebuildInsert() {
//    rangeRebuildChecker.sql("update user_unique_idx set name = 10  where  name = 'tom' and age > 10 ")
//            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]")
//            .check();
  }

  @Test
  public void testRebuildDelete() {
    rangeRebuildChecker.sql("delete from user_unique_idx  where  name = 'tom' and age > 10 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]")
            .check();
  }


  @Test
  public void testRebuildSelect() {
    rangeRebuildChecker.sql("select * from user_unique_idx where  name = 'tom' and age = 10 and  phone = '1212313' and score  = "
            + "99 and remark = 'remark' and salary > 1000.00 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:tom,end:tom},{start:10,end:10},{start:1212313,"
                    + "end:1212313},{start:99.0,end:99.0},{start:1000.00,end:MAX_VALUE}],startInclusive:false,"
                    + "endInclusive:true}]")
            .check();
  }



  // endregion

  class CheckMethod implements TestBase.Checker.CheckMethod {
    @Override
    public void apply(TestBase.Checker checker) {
      try {
        RelOperator relOperator = buildPlanAndOptimize(checker.getSql());
        Assert.assertNotNull(relOperator);
        logger.error(OperatorUtil.printRelOperatorTreeAsc(relOperator, true));
        checker.doCheck(relOperator);
      } catch (JimException e) {
        e.printStackTrace();
        Assert.assertTrue(checker.isHasException());
      }
    }
  }
  class RangeRebuildCheckMethod implements TestBase.Checker.CheckMethod {
    @Override
    public void apply(TestBase.Checker checker) {
      try {
        Operator operator = planAnalyze(checker.getSql());
        Assert.assertNotNull(operator);
//        logger.error(OperatorUtil.printRelOperatorTreeAsc(operator));
        operator.acceptVisitor(new RangeRebuildVisitor(session));
        ((RangeRebuildChecker)checker).checkRange(operator);
      } catch (JimException e) {
        e.printStackTrace();
        Assert.assertTrue(checker.isHasException());
      }
    }
  }

  private Operator planAnalyze(String sql) {
    List<SQLStatement> parse = planner.parse(sql);
    SQLStatement stmt = parse.get(0);
    return planner.analyzeAndOptimize(session, stmt);
  }

  static class RangeRebuildChecker extends TestBase.Checker{
    public RangeRebuildChecker(CheckMethod checkMethod) {
      super(checkMethod);
    }

    public static RangeRebuildChecker build(CheckMethod checkMethod) {
      return new RangeRebuildChecker(checkMethod);
    }

    public void checkRange(Operator operator){
      getCheckPoints().forEach((checkPoint, expected) -> {
        switch (checkPoint) {
          case ColumnRange:
            String range = buildRange(operator);
            Assert.assertEquals(String.format("ColumnRange error . sql : %s ", this.getSql()), expected, range);
            break;
        }
      });
    }

  }
}





