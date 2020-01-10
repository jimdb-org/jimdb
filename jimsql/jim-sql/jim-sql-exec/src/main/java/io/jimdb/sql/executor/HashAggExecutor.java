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
package io.jimdb.sql.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import io.jimdb.core.Session;
import io.jimdb.core.config.SystemProperties;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateFunc;
import io.jimdb.core.expression.aggregate.Cell;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Executor for hash aggregation
 */
@SuppressFBWarnings()
public class HashAggExecutor extends AggExecutor {
  private static final int TASK_PROCESS_ROWS_SIZE = 100;
  private static final int AGG_CONCURRENCY = SystemProperties.getAggExecuteThreads();
  private static final GroupKey NO_GROUP_KEY = new GroupKey(null, 0);
  private static final List<GroupKey> NO_GROUP_KEYS = new ArrayList<>();
  private Schema schema;
  private AggregateFunc[] partialAggFuncs;
  private AggregateFunc[] finalAggFuncs;
  private Expression[] groupByItems;
  private boolean isSerialExecute;

  static {
    NO_GROUP_KEYS.add(NO_GROUP_KEY);
  }

  public HashAggExecutor(Schema schema, AggregateFunc[] partialAggFuncs, AggregateFunc[] finalAggFuncs,
                         Expression[] groupByItems, boolean isSerialExecute) {
    this.schema = schema;
    this.partialAggFuncs = partialAggFuncs;
    this.finalAggFuncs = finalAggFuncs;
    this.groupByItems = groupByItems;
    this.isSerialExecute = isSerialExecute;
  }

  @Override
  public Flux<ExecResult> execute(Session session, ExecResult execResult) {

    if (execResult.size() == 0) {
      ValueAccessor row = initEmptyRow(finalAggFuncs);
      return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]),
              new ValueAccessor[] {row}));
    }

    if (isSingleRun(execResult.size())) {
      return singleRun(session, execResult);
    } else {
      return multipleRun(session, execResult);
    }
  }

  /**
   * isSingleRun: when aggConcurrency = 1 or the row size of child Result is less than rowSize , then single run
   * otherwise multiple run.
   *
   * @param rowSize
   * @return
   */
  private boolean isSingleRun(long rowSize) {
    if (isSerialExecute || AGG_CONCURRENCY <= 1 || rowSize <= TASK_PROCESS_ROWS_SIZE) {
      return true;
    }
    return false;
  }

  private Flux<ExecResult> singleRun(Session session, ExecResult childData) {
    Map<GroupKey, List<Cell>> partialResultMap = new HashMap<>();
    List<GroupKey> groupKeys;
    if (groupByItems == null || groupByItems.length == 0) {
      // no group by
      childData.accept(rows -> calculatePartialResult(session, partialResultMap, NO_GROUP_KEY, rows,
              partialAggFuncs));
      groupKeys = NO_GROUP_KEYS;
    } else {
      groupKeys = new ArrayList<>();
      childData.forEach(row -> {
        GroupKey groupKey = createGroupKey(0, row);

        if (!partialResultMap.containsKey(groupKey)) {
          groupKeys.add(groupKey);
        }

        calculatePartialResult(session, partialResultMap, groupKey, new ValueAccessor[]{ row }, partialAggFuncs);
      });
    }

    List<ValueAccessor> finalResults = new ArrayList<>();

    next:
    for (int rowIdx = 0; rowIdx < groupKeys.size(); rowIdx++) {
      RowValueAccessor rva = new RowValueAccessor(new Value[finalAggFuncs.length]);

      List<Cell> partialResults = getPartialResults(partialResultMap, groupKeys.get(rowIdx), finalAggFuncs);
      for (int columnIdx = 0; columnIdx < finalAggFuncs.length; columnIdx++) {
        if (!finalAggFuncs[columnIdx].append2Result(session, rva, columnIdx, partialResults.get(columnIdx))) {
          continue next;
        }
      }
      finalResults.add(rva);
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]),
            finalResults.toArray(new ValueAccessor[finalResults.size()])));
  }

  private Flux<ExecResult> multipleRun(Session session, ExecResult childData) {

    int taskConcurrency = AGG_CONCURRENCY;

    //calculate task aggConcurrency
    int taskRowSize = (int) Math.ceil(childData.size() * 1.0 / taskConcurrency);

    List<Map<GroupKey, List<List<Cell>>>> finalTaskInputs = buildPartialTasksAndRun(childData, taskConcurrency,
            taskRowSize, session);

    // init final tasks
    FinalTask[] finalTasks = new FinalTask[taskConcurrency];
    for (int i = 0; i < taskConcurrency; i++) {
      finalTasks[i] = new FinalTask(finalTaskInputs.get(i), session);
    }

    //execute final tasks
    return Flux.just(finalTasks)
            .parallel(taskConcurrency)
            .runOn(Schedulers.parallel())
            .flatMap(finalTask -> Flux.just(finalTask.call()))
            .sequential()
            .collectList()
            .map(finalTaskOutputs -> calculateFinalOutputs(session, finalTaskOutputs)).flux();
  }

  private ExecResult calculateFinalOutputs(Session session, List<Map<GroupKey, List<Cell>>> finalTaskOutputs) {
    // build result
    int size = 0;
    for (Map<GroupKey, List<Cell>> output : finalTaskOutputs) {
      size += output.size();
    }
    List<ValueAccessor> result = new ArrayList<>(size);

    for (Map<GroupKey, List<Cell>> output : finalTaskOutputs) {
      next:
      for (List<Cell> valuesInRow : output.values()) {
        ValueAccessor row = new RowValueAccessor(new Value[finalAggFuncs.length]);
        for (int k = 0; k < finalAggFuncs.length; k++) {
          if (!finalAggFuncs[k].append2Result(session, row, k, valuesInRow.get(k))) {
            continue next;
          }
        }
        result.add(row);
      }
    }
    return new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[schema.getColumns().size()]),
            result.toArray(new ValueAccessor[result.size()]));
  }

  private List<Map<GroupKey, List<List<Cell>>>> buildPartialTasksAndRun(ExecResult childData, int taskConcurrency,
                                                                        int taskRowSize, Session session) {
    // split childData and init partial task
    PartialTask[] partialTasks = new PartialTask[taskConcurrency];
    for (int i = 0; i < taskConcurrency; i++) {
      partialTasks[i] = new PartialTask(taskConcurrency, i, taskRowSize, childData, session);
    }
    // execute partialTasks
    return Flux.just(partialTasks)
            .parallel(taskConcurrency)
            .runOn(Schedulers.parallel())
            .flatMap(partialTask -> Flux.just(partialTask.call()))
            .sequential()
            .collectList()
            .map(multiplePartialResults -> getFinalInputs(taskConcurrency, multiplePartialResults))
            .block();
  }

  private List<Map<GroupKey, List<List<Cell>>>> getFinalInputs(int taskConcurrency,
                                                               List<Map<GroupKey, List<Cell>>> multiplePartialResults) {
    // hash data to final tasks
    List<Map<GroupKey, List<List<Cell>>>> finalTaskInputs = new ArrayList<>(taskConcurrency);
    for (int n = 0; n < taskConcurrency; n++) {
      finalTaskInputs.add(new HashMap<>());
    }
    multiplePartialResults.forEach(partialResultMap -> {
      partialResultMap.forEach((groupKey, valuesInRow) -> {
        // shuffle final task input
        Map<GroupKey, List<List<Cell>>> finalTaskInput = finalTaskInputs.get(groupKey.finalTaskIndex);
        List<List<Cell>> rows = finalTaskInput.get(groupKey);
        if (rows == null) {
          rows = new ArrayList<>();
          finalTaskInput.put(groupKey, rows);
        }
        rows.add(valuesInRow);
      });
    });
    return finalTaskInputs;
  }

  private <T> List<Cell> getPartialResults(Map<T, List<Cell>> partialResultMap, T groupKey, AggregateFunc[] aggFuncs) {
    List<Cell> result = partialResultMap.get(groupKey);
    if (result == null) {
      result = initCells(aggFuncs);
      partialResultMap.put(groupKey, result);
    }
    return result;
  }

  private GroupKey createGroupKey(int taskConcurrency, ValueAccessor row) {
    if (groupByItems == null || groupByItems.length == 0) {
      return NO_GROUP_KEY;
    }
    Value[] values = new Value[groupByItems.length];
    for (int i = 0; i < groupByItems.length; i++) {
      if (groupByItems[i].getExprType() == ExpressionType.COLUMN) {
        values[i] = row.get(((ColumnExpr) groupByItems[i]).getOffset());
      } else {
        values[i] = groupByItems[i].exec(row);
      }
    }

    return new GroupKey(values, taskConcurrency);
  }

  private <T> void calculatePartialResult(Session session, Map<T, List<Cell>> result, T groupKey, ValueAccessor[] rows,
                                          AggregateFunc[] aggFuncs) {
    List<Cell> partialResults = getPartialResults(result, groupKey, aggFuncs);

    for (int colIndex = 0; colIndex < aggFuncs.length; colIndex++) {
      Cell partialResult = aggFuncs[colIndex].calculatePartialResult(session, rows,
              partialResults.get(colIndex));
      partialResults.set(colIndex, partialResult);
    }
  }

  /**
   * GroupKey
   */
  private static class GroupKey {
    int finalTaskIndex;
    Value[] groupByValues;

    GroupKey(Value[] groupByValues, int taskConcurrency) {
      this.finalTaskIndex = taskConcurrency > 1 ? hashCode() % taskConcurrency : 0;
      this.groupByValues = groupByValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || !(o instanceof GroupKey && this instanceof GroupKey)) {
        return false;
      }
      GroupKey groupKey = (GroupKey) o;

      if (groupByValues == groupKey.groupByValues) {
        return true;
      }
      if (groupByValues == null || groupKey.groupByValues == null || groupKey.groupByValues.length != groupByValues.length) {
        return false;
      }

      int length = groupByValues.length;
      for (int i = 0; i < length; i++) {
        Value v1 = groupByValues[i];
        Value v2 = groupKey.groupByValues[i];
        if (!(v1 == null ? v2 == null : v1.compareTo(null, v2) == 0)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hashCode() {
//      return Arrays.hashCode(groupByValues);
      if (groupByValues == null) {
        return 0;
      }

      int result = 1;

      for (int i = 0; i < groupByValues.length; i++) {
        result = 31 * result + (groupByValues[i] == null ? 0 : groupByValues[i].hashCode());
      }

      return result;
    }
  }

  /**
   * @version V1.0
   */
  private class PartialTask implements Callable {

    private int taskConcurrency;
    private int taskIndex;
    private int taskRowsLength;
    private ExecResult data;
    private Session session;

    PartialTask(int taskConcurrency, int taskIndex, int taskRowsLength, ExecResult data,
                Session session) {
      this.taskConcurrency = taskConcurrency;
      this.taskIndex = taskIndex;
      this.taskRowsLength = taskRowsLength;
      this.data = data;
      this.session = session;
    }

    @Override
    public Map<GroupKey, List<Cell>> call() {
      Map<GroupKey, List<Cell>> result = new HashMap<>();
      if (taskRowsLength <= 0) {
        return result;
      }
      for (int rowIndex = taskIndex * taskRowsLength; rowIndex < data.size() && rowIndex < (taskIndex + 1) * taskRowsLength; rowIndex++) {
        ValueAccessor row = data.getRow(rowIndex);
        // in big data,hashcode maybe conflict
        GroupKey groupKey = createGroupKey(taskConcurrency, row);

        calculatePartialResult(session, result, groupKey, new ValueAccessor[]{ row }, partialAggFuncs);
      }
      return result;
    }
  }

  /**
   * @version V1.0
   */
  private class FinalTask implements Callable {

    private Map<GroupKey, List<List<Cell>>> input;
    private Session session;

    FinalTask(Map<GroupKey, List<List<Cell>>> input, Session session) {
      this.input = input;
      this.session = session;
    }

    @Override
    public Map<GroupKey, List<Cell>> call() {
      Map<GroupKey, List<Cell>> result = new HashMap<>();
      input.forEach((groupKey, groupKeyRows) -> {
//        List<Cell> cells = getPartialResults(result, groupKey, finalAggFuncs);
        List<Cell> cells = result.get(groupKey);
        for (List<Cell> row : groupKeyRows) {
          if (cells == null) {
            cells = row;
            result.put(groupKey, row);
            continue;
          }
          for (int i = 0; i < finalAggFuncs.length; i++) {
            Cell ret = finalAggFuncs[i].mergePartialResult(session, row.get(i), cells.get(i));
            cells.set(i, ret);
          }
        }
      });
      return result;
    }
  }


}
