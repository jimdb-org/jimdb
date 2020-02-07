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

package io.jimdb.test.planner.statistics.unit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionUtil;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

public class StatUsageTest extends StatUsageBaseTest {

  @Test
  public void testCBOWithTwoIndexFilters() {
    Checker checker = Checker.build()
            .sql("select name, age from user where age > 20 and salary > 7000")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user) -> TableSource(user) -> "
                            + "Selection(GreaterInt(test.user.age,20)) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownIndexPlan,
                    "IndexSource(user.idx_salary)");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyInt(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 1) {
                        // idx_age
                        return 7.0;
                      }
                      if (indexId == 4) {
                        // idx_salary
                        return 2.0;
                      }
                      return 5.0;
                    }
            );
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCBOWithBetterTablePlan() {
    Checker checker = Checker.build()
            .sql("select name, age, score from user where age > 20")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user)")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[name,age,score], pushDownPredicates=[GreaterInt(test.user.age,20)]}");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyInt(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 1) {
                        // idx_age
                        return 10.0;
                      }
                      return 10.0;
                    }
            );

    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      List<Expression> indexFilters = (List<Expression>) invocation.getArgument(1);
                      List<ColumnExpr> outExprs = new ArrayList<>();
                      ExpressionUtil.extractColumns(outExprs, indexFilters, null);
                      long columnId = outExprs.get(0).getId();
                      if (columnId == 1) {
                        // age
                        return 0.7;
                      }
                      if (columnId == 5) {
                        // salary
                        return 0.2;
                      }
                      return 1.0;
                    }
            );

    run(checker);
  }

  @Test
  public void testCBOWithBetterIndexPlan() {
    Checker checker = Checker.build()
            .sql("select name, age, score from user where salary > 7000")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user) -> TableSource(user) -> IndexLookUp -> Projection")
            .addCheckPoint(CheckPoint.PushDownIndexPlan,
                    "IndexSource(user.idx_salary)");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyInt(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 4) {
                        // idx_salary
                        return 2.0;
                      }
                      return 5.0;
                    }
            );

    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      List<Expression> indexFilters = (List<Expression>) invocation.getArgument(1);
                      List<ColumnExpr> outExprs = new ArrayList<>();
                      ExpressionUtil.extractColumns(outExprs, indexFilters, null);
                      long columnId = outExprs.get(0).getId();
                      if (columnId == 1) {
                        // age
                        return 0.7;
                      }
                      if (columnId == 4) {
                        // salary
                        return 0.2;
                      }
                      return 1.0;
                    }
            );

    run(checker);
  }

  @Test
  public void testCBOWithIndexReadOnly() {
    Checker checker = Checker.build()
            .sql("select name, salary from user where salary > 7000")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user)")
            .addCheckPoint(CheckPoint.IndexPlan, "IndexSource(user.idx_salary)");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyInt(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 4) {
                        // idx_salary
                        return 2.0;
                      }
                      return 1.0;
                    }
            );

    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      List<Expression> indexFilters = (List<Expression>) invocation.getArgument(1);
                      List<ColumnExpr> outExprs = new ArrayList<>();
                      ExpressionUtil.extractColumns(outExprs, indexFilters, null);
                      long columnId = outExprs.get(0).getId();
                      if (columnId == 1) {
                        // age
                        return 0.7;
                      }
                      if (columnId == 5) {
                        // salary
                        return 0.2;
                      }
                      return 1.0;
                    }
            );

    run(checker);
  }
}
