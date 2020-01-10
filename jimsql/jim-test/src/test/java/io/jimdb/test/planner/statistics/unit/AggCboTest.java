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

package io.jimdb.test.planner.statistics.unit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import io.jimdb.test.planner.TestBase;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

public class AggCboTest extends StatUsageBaseTest {

  @Test
  public void testCboAggWithoutGrby() {
    TestBase.Checker checker = Checker.build()
            .sql("select sum(score) from user")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> StreamAgg(SUM(col_0))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> StreamAgg(SUM(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountWithoutGrby() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(*) from user")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> StreamAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[name]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> StreamAgg(COUNT(1))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggAverage() {
    TestBase.Checker checker = Checker.build()
            .sql("select avg(score) from user group by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(AVG(col_0,col_1))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> HashAgg(SUM(test.user.score),COUNT(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggSumGrbyCol() {
    TestBase.Checker checker = Checker.build()
            .sql("select sum(score) from user group by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(SUM(col_0))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> HashAgg(SUM(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggSumWhere() {
    TestBase.Checker checker = Checker.build()
            .sql("select sum(salary) from user where age > 30 group by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(SUM(col_0))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[age,score,salary], pushDownPredicates=[GreaterInt(test.user.age,30)]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> Selection(GreaterInt(test.user.age,30)) -> HashAgg(SUM(test.user.salary))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggSumGrbyIndexCol() {
    TestBase.Checker checker = Checker.build()
            .sql("select sum(score) from user group by age")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> StreamAgg(SUM(col_0))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[age,score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> StreamAgg(SUM(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyLong(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 1) {
                        // idx_age
                        return 2.0;
                      }
                      return 10.0;
                    }
            );
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggAvgWhereGrbyIndexCol() {
    TestBase.Checker checker = Checker.build()
            .sql("select avg(salary) from user where score < 80 group by age")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> StreamAgg(AVG(col_0,col_1))")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[age,score,salary], pushDownPredicates=[LessInt(test.user.score,80)]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> Selection(LessInt(test.user.score,80)) -> StreamAgg(SUM(test.user.salary),COUNT(test.user.salary))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountIndexOnly() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(salary) from user group by salary")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user) -> StreamAgg(COUNT(col_0))")
            .addCheckPoint(CheckPoint.IndexPlan, "IndexSource(user.idx_salary)");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyLong(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 4) {
                        // idx_salary
                        return 2.0;
                      }
                      return 10.0;
                    }
            );
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountLimitGrbyIndexCol() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(*) from user group by salary limit 3")
            .addCheckPoint(CheckPoint.PlanTree,
                    "IndexSource(user) -> StreamAgg(COUNT(col_0)) -> Limit")
            .addCheckPoint(CheckPoint.IndexPlan, "IndexSource(user.idx_salary)");

    when(tableSourceStatInfo.estimateRowCountByIndexedRanges(any(), anyLong(), anyList())).
            thenAnswer((InvocationOnMock invocation) -> {
                      long indexId = (long) invocation.getArgument(1);
                      if (indexId == 4) {
                        // idx_salary
                        return 2.0;
                      }
                      return 10.0;
                    }
            );
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountLimit() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(*) from user group by score limit 6")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(COUNT(col_0)) -> Limit")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> HashAgg(COUNT(1))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountOrder() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(*) from user group by score order by score")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(COUNT(col_0),DISTINCT(col_1)) -> Order -> Projection")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> HashAgg(COUNT(1),DISTINCT(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }

  @Test
  public void testCboAggCountOrderLimit() {
    TestBase.Checker checker = Checker.build()
            .sql("select count(*) from user group by score order by score limit 5")
            .addCheckPoint(CheckPoint.PlanTree,
                    "TableSource(user) -> HashAgg(COUNT(col_0),DISTINCT(col_1)) -> "
                            + "TopN(Exprs=(test.user.score,ASC),Offset=0,Count=5) -> Projection")
            .addCheckPoint(CheckPoint.TablePlan,
                    "TableSource={columns=[score]}")
            .addCheckPoint(CheckPoint.PushDownTablePlan,
                    "TableSource(user) -> HashAgg(COUNT(1),DISTINCT(test.user.score))");

    when(tableSourceStatInfo.estimateRowCountByIntColumnRanges(any(), anyLong(), anyList())).thenReturn(10.0);
    when(tableSourceStatInfo.calculateSelectivity(any(), anyList())).thenReturn(1.0);

    run(checker);
  }
}
