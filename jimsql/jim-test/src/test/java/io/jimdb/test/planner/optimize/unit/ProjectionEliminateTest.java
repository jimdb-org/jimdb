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

import java.util.LinkedHashMap;
import java.util.Map;

import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class ProjectionEliminateTest extends TestBase {

  @Test
  public void projectionEliminateTest() {

    Map<String, String> projections = new LinkedHashMap<>();
    projections.put("select * from user order by user.name limit 5",
            "TableSource(user) -> Projection -> Order -> Limit"
    );

    projections.put("select name as c1, age as c2 from user order by 1, phone + phone limit 2",
            "TableSource(user) -> Order -> Limit -> Projection"
    );

    projections.put("select count(*) from user where age = 10 and age = 123456 order by 1, phone + phone limit 1",
            "TableSource(user) -> Selection(EqualInt(test.user.age,10),EqualInt(test.user.age,123456)) -> HashAgg(COUNT(1),DISTINCT(test.user.phone)) -> Order -> Limit -> Projection"
    );

    projections.put("select count(*) from user order by 1, phone + phone limit 1",
            "TableSource(user) -> HashAgg(COUNT(1),DISTINCT(test.user.phone)) -> Order -> Limit -> Projection"
    );


    projections.forEach((k, v) -> {
      RelOperator logicalPlan = buildPlanTree(k);
      logicalPlan = LogicalOptimizer.optimize(logicalPlan,
              OptimizeFlag.PRUNCOLUMNS | OptimizeFlag.ELIMINATEPROJECTION, session);
      String tree = OperatorUtil.printRelOperatorTree(logicalPlan);
      Assert.assertEquals("not eq.", v, tree);
      System.out.println("[" + k + "]  after : projectionEliminateTest \n" + tree + "\n");
    });
  }
}
