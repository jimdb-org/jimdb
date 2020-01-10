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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.Schema;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * BuildKeyTest
 *
 * @since 2019-08-20
 */
public class BuildKeyTest extends TestBase {

  private List<SqlAndKeyColumns> testCases = new ArrayList<>();


  @Test
  public void testBuildKeyColumn() {
    Map<String, String[][]> testBuildKeyColumnMap = new HashMap<>();
    testBuildKeyColumnMap.put("Projection", new String[][]{
            {"test.user.phone"}, {"test.user.age", "test.user.phone"}
    });
    testBuildKeyColumnMap.put("Aggregation", new String[][]{
            {"test.user.phone"}, {"test.user.age", "test.user.phone"}, {"test.user.name"}
    });
    testBuildKeyColumnMap.put("TableSource", new String[][]{
            {"test.user.phone"}, {"test.user.age", "test.user.phone"}, {"test.user.name"}
    });

    testCases.add(new SqlAndKeyColumns("select phone, age, sum(name) from user", testBuildKeyColumnMap));

    Map<String, String[][]> testBuildKeyColumnMap1 = new HashMap<>();
    testBuildKeyColumnMap1.put("Projection", new String[][]{
            {"test.user.name"}
    });
    testBuildKeyColumnMap1.put("TableSource", new String[][]{
            {"test.user.phone"}, {"test.user.age", "test.user.phone"}, {"test.user.name"}
    });

    testCases.add(new SqlAndKeyColumns("select name from user where age = 10 order by name desc limit 5", testBuildKeyColumnMap1));

    testCases.forEach(sqlAndKeyColumns -> {
      RelOperator logicalPlan = buildPlanTree(sqlAndKeyColumns.getSql());
      String treeBeforeOptimize = OperatorUtil.printRelOperatorTree(logicalPlan);

      logicalPlan = LogicalOptimizer.optimize(logicalPlan, OptimizeFlag.BUILDKEYINFO, session);

      checkKeyColumns(logicalPlan, sqlAndKeyColumns);

      String treeAfterOptimize = OperatorUtil.printRelOperatorTree(logicalPlan);
      Assert.assertEquals("LogicPlanTree changed after building key info.", treeBeforeOptimize, treeAfterOptimize);
      System.out.println(treeAfterOptimize);
    });


  }

  private void checkKeyColumns(RelOperator operator, SqlAndKeyColumns sqlAndKeyColumns) {
    if (sqlAndKeyColumns == null) {
      return;
    }

    String op = "";
    if (operator instanceof Projection) {
      op = "Projection";
    } else if (operator instanceof Aggregation) {
      op = "Aggregation";
    } else if (operator instanceof TableSource) {
      op = "TableSource";
    } else if (operator instanceof Selection) {
      op = "Selection";
    } else if (operator instanceof Order) {
      op = "Order";
    } else if (operator instanceof Limit) {
      op = "Limit";
    }

    Schema schema = operator.getSchema();
    String[][] keyss = sqlAndKeyColumns.getTestBuildKeyColumnMap().get(op);
    if (keyss == null) {
//      Assert.assertEquals(0, schema.getKeyColumns().size());
    } else {
      List<KeyColumn> keyColumnList = schema.getKeyColumns();
      for (int i = 0; i < keyss.length; i++) {
        String[] keys = keyss[i];
        for (int j = 0; j < keys.length; j++) {
          String key = keys[j];
          Assert.assertEquals("not eq.", key, keyColumnList.get(i).getColumnExprs().get(j).toString());
        }
      }
    }

    if (operator.getChildren() != null) {
      for (RelOperator child : operator.getChildren()) {
        checkKeyColumns(child, sqlAndKeyColumns);
      }
    }
  }


  static class SqlAndKeyColumns {
    private String sql;
    private Map<String, String[][]> testBuildKeyColumnMap;

    public SqlAndKeyColumns(String sql, Map<String, String[][]> testBuildKeyColumnMap) {
      this.sql = sql;
      this.testBuildKeyColumnMap = testBuildKeyColumnMap;
    }

    public String getSql() {
      return sql;
    }

    public Map<String, String[][]> getTestBuildKeyColumnMap() {
      return testBuildKeyColumnMap;
    }
  }

}
