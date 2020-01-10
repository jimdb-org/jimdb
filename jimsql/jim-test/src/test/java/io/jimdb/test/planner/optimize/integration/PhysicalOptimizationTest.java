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
import io.jimdb.core.expression.Schema;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.statistics.TableStats;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * PhysicalOptimizationTest
 *
 * @since 2019/8/27
 */

public class PhysicalOptimizationTest extends TestBase {

  @Before
  public void init() {

    Table table = MetaData.Holder.getMetaData().getTable("test", "person");

    TableStats tableStats = TableStatsManager.getTableStats(table);

    if (tableStats == null) {
      Schema schema = new Schema(session, table.getReadableColumns());
      TableStatsManager.addToCache(table, new TableStats(session, table, schema, 0));
    }

  }

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
  public void test1() {
    Checker checker = Checker.build()
//            .sql("select * from person")
            .sql("select * from person where id = 1 or age =  2")
//            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
//                    + "endInclusive:true}]]")
//            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person)");
//            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
//            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");


    run(checker);
  }

  @Test
  public void test2() {
    Checker checker = Checker.build()
            .sql("select name from person")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
                    + "endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test3() {

    Checker checker = Checker.build()
            .sql("select u.* from person u")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
                    + "endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test4() {
    Checker checker = Checker.build()
            .sql("select u.* from person as u")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
                    + "endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(u)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test5() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test6() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 and score=100")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualInt(test.person.score,100)],accessConditions=[]\","
                    + "\"idx_age\":\"tableConditions=[EqualInt(test.person.score,100)],accessConditions=[EqualInt"
                    + "(test.person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1), EqualInt"
                    + "(test.person.score,100)],accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[EqualInt"
                    + "(test.person.score,100)],accessConditions=[EqualInt(test.person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> Selection(EqualInt(test"
                    + ".person.score,100)) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE -> SELECTION_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "score EqualInt value(100) ");

    run(checker);
  }

  @Test
  public void test7() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age = 1 or score = 100")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(EqualInt(test.person"
                    + ".age,1),EqualInt(test.person.score,100))],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt value(1) LogicOr score EqualInt value(100)");

    run(checker);
  }

  @Test
  public void test8() {
    Checker checker = Checker.build()
            .sql("select id from person where age = score")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,"
                    + "test.person.score)],accessConditions=[]\",\"idx_age\":\"tableConditions=[EqualInt(test.person"
                    + ".age,test.person.score)],accessConditions=[]\",\"idx_phone\":\"tableConditions=[EqualInt(test"
                    + ".person.age,test.person.score)],accessConditions=[]\","
                    + "\"idx_age_phone\":\"tableConditions=[EqualInt(test.person.age,test.person.score)],"
                    + "accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt score");

    run(checker);
  }

  @Test
  public void test9() {
    Checker checker = Checker.build()
            .sql("select id from person where 1=1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
                    + "endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test10() {
    Checker checker = Checker.build()
            .sql("select id from person where 1=1 and age=1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test11() {
    Checker checker = Checker.build()
            .sql("select id,age from person where name ='Tom' and (age =1 or score =100)")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualString(test.person.name,Tom), "
                    + "LogicOr(EqualInt(test.person.age,1),EqualInt(test.person.score,100))],accessConditions=[]\","
                    + "\"idx_age\":\"tableConditions=[EqualString(test.person.name,Tom), LogicOr(EqualInt(test.person.age,1),EqualInt(test.person.score,100))],accessConditions=[]\","
                    + "\"idx_phone\":\"tableConditions=[EqualString(test.person.name,Tom), LogicOr(EqualInt(test.person.age,1),EqualInt(test.person.score,100))],accessConditions=[]\","
                    + "\"idx_age_phone\":\"tableConditions=[EqualString(test.person.name,Tom), LogicOr(EqualInt(test.person.age,1),EqualInt(test.person.score,100))],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "name EqualString value(Tom) AND age EqualInt value(1) "
                    + "LogicOr score EqualInt value(100)");

    run(checker);
  }

  @Test
  public void test12() {
    Checker checker = Checker.build()
            .sql("select sum(1) from person")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> HashAgg(SUM(col_0))")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> AGGREGATION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test13() {
    Checker checker = Checker.build()
            .sql("select count(1+1) from person")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> HashAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> AGGREGATION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test14() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age=1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test15() {
    Checker checker = Checker.build()
            .sql("select name as c1,age a from person where age=1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test.person.age,1)]\","
                    + "\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],accessConditions=[]\","
                    + "\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test.person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test16() {
    Checker checker = Checker.build()
            .sql("select *,1 from person")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test17() {
    Checker checker = Checker.build()
            .sql("select 1, person.* from person")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test18() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 order by age desc")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection "
                    + "-> Order")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test19() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 order by age desc,name desc")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection "
                    + "-> Order")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test20() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 order by 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection "
                    + "-> Order")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test21() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 order by 1+1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection "
                    + "-> Order")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test22() {
    Checker checker = Checker.build()
            .sql("select name,age from person where age=1 order by (age+score)")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Order -> "
                    + "Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test23() {
    Checker checker = Checker.build()
            .sql("select id from person limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Limit")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> LIMIT_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test24() {
    Checker checker = Checker.build()
            .sql("select id from person where age=1 order by score limit 10")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> TopN(Exprs=(test.person"
                    + ".score,ASC),Offset=0,Count=10) -> IndexLookUp -> TopN(Exprs=(test.person.score,ASC),Offset=0,"
                    + "Count=10) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE -> ORDER_BY_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test25() {
    Checker checker = Checker.build()
            .sql("select id from person where age=18 and phone='123'")
            .addCheckPoint(CheckPoint.ColumnRange, "")
            .addCheckPoint(CheckPoint.DetachConditions, "{idx_age_phone:[18123]}")
            .addCheckPoint(CheckPoint.PlanTree, "KeyGet((idx_age_phone(age,phone),values(18:LONG,123:STRING))) -> "
                    + "Projection(test.person.id)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test26() {
    Checker checker = Checker.build()
            .sql("select age from person where name='Tom' or name='Jack'")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(EqualString(test.person"
                    + ".name,Tom),EqualString(test.person.name,Jack))],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "name EqualString value(Tom) LogicOr name EqualString "
                    + "value(Jack)");

    run(checker);
  }

  @Test
  public void test27() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where id = 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{primary:[1]}")
            .addCheckPoint(CheckPoint.PlanTree, "KeyGet((primary(id),values(1:LONG))) -> Projection(test.person.id,test"
                    + ".person.name,test.person.age)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test28() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where age = 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)"
                    + "],accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test"
                    + ".person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test29() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where id = 1 and age = 1 and score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualInt(test.person.score,1)],accessConditions=[EqualInt(test.person.id,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt value(1) AND score EqualInt value(1)");

    run(checker);
  }

  @Test
  public void test30() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where id = 1 or age = 1 or score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(LogicOr(EqualInt"
                    + "(test.person.id,1),EqualInt(test.person.age,1)),EqualInt(test.person.score,1))],"
                    + "accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "id EqualInt value(1) LogicOr age EqualInt value(1) "
                    + "LogicOr score EqualInt value(1)");

    run(checker);
  }

  @Test
  public void test31() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where score = 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.score,"
                    + "1)],accessConditions=[]\",\"idx_age\":\"tableConditions=[EqualInt(test.person.score,1)],"
                    + "accessConditions=[]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.score,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[EqualInt(test.person.score,1)],"
                    + "accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "score EqualInt value(1)");

    run(checker);
  }

  @Test
  public void test32() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where name = 'Tom' ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualString(test.person"
                    + ".name,Tom)],accessConditions=[]\",\"idx_age\":\"tableConditions=[EqualString(test.person.name,"
                    + "Tom)],accessConditions=[]\",\"idx_phone\":\"tableConditions=[EqualString(test.person.name,Tom)"
                    + "],accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[EqualString(test.person.name,Tom)"
                    + "],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "name EqualString value(Tom)");

    run(checker);
  }

  @Test
  public void test33() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where age = 1 and name = 'Tom'")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualString(test.person.name,Tom)],accessConditions=[]\","
                    + "\"idx_age\":\"tableConditions=[EqualString(test.person.name,Tom)],accessConditions=[EqualInt"
                    + "(test.person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1), "
                    + "EqualString(test.person.name,Tom)],accessConditions=[]\","
                    + "\"idx_age_phone\":\"tableConditions=[EqualString(test.person.name,Tom)],"
                    + "accessConditions=[EqualInt(test.person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> Selection(EqualString(test"
                    + ".person.name,Tom)) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE -> SELECTION_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "name EqualString value(Tom) ");

    run(checker);
  }

  @Test
  public void test34() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where age = 1 or name = 'Tom'")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(EqualInt(test.person"
                    + ".age,1),EqualString(test.person.name,Tom))],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt value(1) LogicOr name EqualString value"
                    + "(Tom)");

    run(checker);
  }

  @Test
  public void test35() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where phone = 1123")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,"
                    + "endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{idx_phone:[1123]}")
            .addCheckPoint(CheckPoint.PlanTree, "KeyGet((idx_phone(phone),values(1123:STRING))) -> Projection(test.person"
                    + ".id,test.person.name,test.person.age)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test36() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where id = 1 and age = 1123 or score = 23")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(LogicAnd(EqualInt"
                    + "(test.person.id,1),EqualInt(test.person.age,1123)),EqualInt(test.person.score,23))],"
                    + "accessConditions=[]\",\"idx_age\":\"tableConditions=[LogicOr(LogicAnd(EqualInt(test.person.id,"
                    + "1),EqualInt(test.person.age,1123)),EqualInt(test.person.score,23))],accessConditions=[]\","
                    + "\"idx_age_phone\":\"tableConditions=[LogicOr(LogicAnd(EqualInt(test.person.id,1),EqualInt(test"
                    + ".person.age,1123)),EqualInt(test.person.score,23))],accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "id EqualInt value(1) LogicAnd age EqualInt value(1123) "
                    + "LogicOr score EqualInt value(23)");

    run(checker);
  }

  @Test
  public void test37() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where  age = 1123 or score = 23 and ( name = 'Tom' ) ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(EqualInt(test.person"
                    + ".age,1123),LogicAnd(EqualInt(test.person.score,23),EqualString(test.person.name,Tom)))],"
                    + "accessConditions=[]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt value(1123) LogicOr score EqualInt value"
                    + "(23) LogicAnd name EqualString value(Tom)");

    run(checker);
  }

  @Test
  public void test38() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where id = 1 and age = 1 and score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualInt(test.person.score,1)],accessConditions=[EqualInt(test.person.id,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "age EqualInt value(1) AND score EqualInt value(1)");

    run(checker);
  }

  @Test
  public void test39() {
    Checker checker = Checker.build()
            .sql("select count(*) from person where id = 1 and score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.score,1)"
                    + "],accessConditions=[EqualInt(test.person.id,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> HashAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.PushDownProcessors, "TABLE_READ_TYPE -> SELECTION_TYPE -> AGGREGATION_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "score EqualInt value(1)");

    run(checker);
  }

  @Test
  public void test40() {
    Checker checker = Checker.build()
            .sql("select age from person where age = 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test41() {
    Checker checker = Checker.build()
            .sql("select age from person where age = 1 order by id limit 10")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            // TopN doesn't have accessPath
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TopN(Exprs=(test.person.id,ASC),Offset=0,"
                    + "Count=10) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test42() {
    Checker checker = Checker.build()
            .sql("select id,name,age from person where age = 1 and score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualInt(test.person.score,1)],accessConditions=[]\",\"idx_age\":\"tableConditions=[EqualInt"
                    + "(test.person.score,1)],accessConditions=[EqualInt(test.person.age,1)]\","
                    + "\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1), EqualInt(test.person.score,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[EqualInt(test.person.score,1)],"
                    + "accessConditions=[EqualInt(test.person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> Selection(EqualInt(test"
                    + ".person.score,1)) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE -> SELECTION_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "score EqualInt value(1) ");

    run(checker);
  }

  @Test
  public void test43() {
    Checker checker = Checker.build()
            .sql("select count(*) from person where age = 1 and score = 1 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1),"
                    + " EqualInt(test.person.score,1)],accessConditions=[]\",\"idx_age\":\"tableConditions=[EqualInt"
                    + "(test.person.score,1)],accessConditions=[EqualInt(test.person.age,1)]\","
                    + "\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1), EqualInt(test.person.score,1)],"
                    + "accessConditions=[]\",\"idx_age_phone\":\"tableConditions=[EqualInt(test.person.score,1)],"
                    + "accessConditions=[EqualInt(test.person.age,1)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> Selection(EqualInt(test"
                    + ".person.score,1)) -> HashAgg(COUNT(1)) -> IndexLookUp -> HashAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> "
                    + "tablePlanProcessors : [TABLE_READ_TYPE -> SELECTION_TYPE -> AGGREGATION_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "score EqualInt value(1) ");

    run(checker);
  }

  @Test
  public void test44() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age=1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,"
                    + "Count=5)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }


  @Test
  public void test45() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age > 1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:MAX_VALUE,startInclusive:false,endInclusive:true}]]");
//            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)],"
//                    + "accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test.person"
//                    + ".age,1)]\"}")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,Count=5)")
//            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
//            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }


  @Test
  public void test455() {
    Checker checker = Checker.build()
            .sql("select id,age,name from person where age > 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:1,end:MAX_VALUE}],startInclusive:false,endInclusive:true}]");
//            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1)],"
//                    + "accessConditions=[]\",\"idx_age\":\"tableConditions=[],accessConditions=[EqualInt(test.person"
//                    + ".age,1)]\"}")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,Count=5)")
//            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
//            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }


  @Test
  public void test46() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age >= 1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,"
                    + "Count=5)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test47() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age < 1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:1,startInclusive:true,endInclusive:false}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,"
                    + "Count=5)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  @Test
  public void test48() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age <= 1 order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:1,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "null")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TopN(Exprs=(test.person.age,DESC),Offset=0,"
                    + "Count=5)")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }

  /**
   * Multiple single-column indexes
   */
  @Test
  public void test49() {
    Checker checker = Checker.build()
            .sql("select id,age from person where age = 1 and phone > '123' order by age desc limit 5")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:false,endInclusive:true}{start:123,end:MAX_VALUE,startInclusive:false,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[EqualInt(test.person.age,1), "
                    + "GreaterString(test.person.phone,123)],accessConditions=[]\","
                    + "\"idx_age\":\"tableConditions=[GreaterString(test.person.phone,123)],accessConditions=[EqualInt"
                    + "(test.person.age,1)]\",\"idx_phone\":\"tableConditions=[EqualInt(test.person.age,1)],"
                    + "accessConditions=[GreaterString(test.person.phone,123)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> Selection(GreaterString(test.person.phone,123)) -> TopN(Exprs=(test.person.age,DESC),Offset=0,Count=5) -> IndexLookUp -> TopN(Exprs=(test.person.age,DESC),Offset=0,Count=5) -> Projection")
            .addCheckPoint(CheckPoint.PushDownProcessors, "INDEX_READ_TYPE -> ORDER_BY_TYPE")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }



  @Test
  public void test50() {
    Checker checker = Checker.build()
            .sql("select id,age,name from person where age = 1 or age = 2 ")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:2,startInclusive:true,endInclusive:true}]]")
            .addCheckPoint(CheckPoint.DetachConditions, "{\"primary\":\"tableConditions=[LogicOr(EqualInt(test.person"
                    + ".age,1),EqualInt(test.person.age,2))],accessConditions=[]\",\"idx_age\":\"tableConditions=[],"
                    + "accessConditions=[EqualInt(test.person.age,1), EqualInt(test.person.age,2)]\","
                    + "\"idx_age_phone\":\"tableConditions=[],accessConditions=[EqualInt(test.person.age,1), EqualInt"
                    + "(test.person.age,2)]\"}")
            .addCheckPoint(CheckPoint.PlanTree, "IndexSource(person) -> TableSource(person) -> IndexLookUp")
            .addCheckPoint(CheckPoint.PushDownProcessors, "indexPlanProcessors : [INDEX_READ_TYPE] -> tablePlanProcessors : [TABLE_READ_TYPE]")
            .addCheckPoint(CheckPoint.ProcessorpbSelection, "");

    run(checker);
  }


  @Test
  public void test51() {
    Checker checker = Checker.build()
            .sql("select id,age,name from person where age = 1 or age = 2 or age = 3  or age = 4")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:4,startInclusive:true,endInclusive:true}]]")
            ;

    run(checker);
  }

  @Test
  public void test52() {
    Checker checker = Checker.build()
            .sql("select id,age,name from person where id = 1 or id = 2 or id = 3  or id = 4")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:1,end:1,startInclusive:true,endInclusive:true}][{start:2,end:2,startInclusive:true,endInclusive:true}][{start:3,end:3,startInclusive:true,endInclusive:true}][{start:4,end:4,startInclusive:true,endInclusive:true}]]")
            ;

    run(checker);
  }

  @Test
  public void test53() {
    Checker checker = Checker.build()
            .sql("select id,age,name from person where age = 1 or score = 1")
            .addCheckPoint(CheckPoint.ColumnRange, "[[{start:MIN_VALUE,end:MAX_VALUE,startInclusive:true,endInclusive:true}]]")
            ;

    run(checker);
  }



  @Test
  public void test54() {
    Checker checker = Checker.build()
        .sql("select id,age,name from user where age = null ")
        .addCheckPoint(CheckPoint.ColumnRange, "[{[{start:MIN_VALUE,end:MAX_VALUE}],startInclusive:true,endInclusive:true}]")
        ;

    run(checker);
  }


  private List<Checker> buildAllTestCases() {
    List<Checker> checkerList = new ArrayList<>();

    // aggregation
//    checkerList.add(Checker.build()
//            .sql("select sum(1) from person where age=1")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//    checkerList.add(Checker.build()
//            .sql("select name,age,sum(1) from person where age=1")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//    checkerList.add(Checker.build()
//            .sql("select id,age,count(*) from person where age=1")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );

//    checkerList.add(Checker.build()
//            .sql("select count(1)+sum(age)+sum(score) from person where score=100 and (name='Tom' or age=10)")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//

    // TODO FIX
//    checkerList.add(Checker.build()
//            .sql("select id from person order by count(id)")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//    checkerList.add(Checker.build()
//            .sql(" select id as ff from person where age=1 order by count(id) desc")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//    checkerList.add(Checker.build()
//            .sql("select name,count(score) from person where score=100 and (name='Tom' or age=10) order by sum(name)")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );

    // limit

    // use/force index
//    checkerList.add(Checker.build()
//            .sql("select age from person use index(idx_age_phone)")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,""));
    // hasException
//    checkerList.add(Checker.build()
//            .sql("select age from person use index(idx_age) force index(idx_age_x)")
//            .addCheckPoint(CheckPoint.PlanTree, DBException.get(ErrorModule.META,
//                    ErrorCode.ER_KEY_COLUMN_DOES_NOT_EXISTS, "idx_age_x").toString())
//            .hasException(true));

    // distinct
//    checkerList.add(Checker.build()
//            .sql("select distinct name from person")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );
//    checkerList.add(Checker.build()
//            .sql("select distinct name from person where age = 10 limit 10")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );

    // compositive
//    checkerList.add(Checker.build()
//            .sql("select count(1)+sum(age) from person where score=100 and (name='Tom' or age=10) order by sum(score)
//            +1")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );

//    checkerList.add(Checker.build()
//            .sql("select phone,count(age),count(name)+sum(age),sum(age+score) from person u " +
//                    "where score=age+10 and (name='Tom' or age=10) order by sum(score)+1 limit 1,5")
//            .addCheckPoint(CheckPoint.ColumnRange, "")
//            .addCheckPoint(CheckPoint.DetachConditions,"")
//            .addCheckPoint(CheckPoint.PlanTree, "TableSource(person) -> Projection")
//            .addCheckPoint(CheckPoint.PushDownProcessors,"")
//    );

    return checkerList;
  }
}
