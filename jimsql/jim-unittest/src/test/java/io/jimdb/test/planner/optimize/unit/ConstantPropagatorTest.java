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

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.expression.Expression;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.optimizer.ConstantPropagator;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

public class ConstantPropagatorTest extends TestBase {

  @Test
  public void testComposite_Case1() {
    String sql = "select name from user where score=age and age=10";

    try {
      RelOperator planTree = buildPlanTree(sql);
      Selection selection = (Selection) planTree.getChildren()[0];
      List<Expression> expressions = selection.getConditions();
      ConstantPropagator constantPropagator = new ConstantPropagator(expressions, session);
      List<Expression> result = constantPropagator.propagateConstants();
      String resultStr = predicates2str(result, "");
      Assert.assertEquals("=[FUNC-EqualInt(score,10),FUNC-EqualInt(age,10)]", resultStr);
    } catch (BaseException e) {
      Assert.fail(e.getMessage());
    }
  }

}
