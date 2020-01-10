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
import java.util.List;
import java.util.Map;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Schema;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class ColumnPruneTest extends TestBase {

  @Test
  public void columnPruneOptimizerTest() {

    Map<String, String[]> pruneMap = new LinkedHashMap<>();
    pruneMap.put("select phone from user", new String[]
            { "phone" }
    );

    pruneMap.put("select count(*) from user", new String[]
            { "name" }
    );

    pruneMap.put("select name as c1, age as c2 from user order by phone + phone", new String[]
            { "name", "age", "phone" }
    );

    pruneMap.keySet().forEach(sql -> {
      RelOperator logicalPlan = buildPlanTree(sql);
      logicalPlan = LogicalOptimizer.optimize(logicalPlan, OptimizeFlag.PRUNCOLUMNS, session);
      checkPrune(logicalPlan, pruneMap.get(sql));
    });
  }

  private void checkPrune(RelOperator relOperator, String... cols) {
    if (relOperator instanceof TableSource) {
      TableSource tableSource = (TableSource) relOperator;
      Schema schema = tableSource.getSchema();
      List<ColumnExpr> columns = schema.getColumns();
      Assert.assertEquals(columns.size(), cols.length);
      for (int i = 0; i < columns.size(); i++) {
        Assert.assertEquals(columns.get(i).getOriCol(), cols[i]);
      }
    }

    if (relOperator.getChildren() != null) {
      checkPrune(relOperator.getChildren()[0], cols);
    }
  }
}
