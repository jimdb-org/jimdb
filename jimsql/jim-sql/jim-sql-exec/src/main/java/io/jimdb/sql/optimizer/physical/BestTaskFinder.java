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

package io.jimdb.sql.optimizer.physical;

import static io.jimdb.core.expression.ExpressionUtil.extractColumnIds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.ParameterizedOperatorVisitor;
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;
import io.jimdb.sql.optimizer.statistics.StatsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class to find the best task according to the given logical operator (logical plan)
 */
@SuppressFBWarnings()
public class BestTaskFinder extends ParameterizedOperatorVisitor<PhysicalProperty, Task> {

  private static final Logger LOG = LoggerFactory.getLogger(BestTaskFinder.class);
  private static PhysicalOperatorGenerator physicalOperatorGenerator = new PhysicalOperatorGenerator();
  private static OperatorToTaskAttacher operatorToTaskAttacher = new OperatorToTaskAttacher();

  @Override
  public Task visitOperatorByDefault(Session session, RelOperator operator, PhysicalProperty prop) {
    Task bestTask = ProxyTask.initProxyTask();

    // convert one logical plan to at least one physical plan
    // TODO currently this process is only one-to-one, and actually we can simply replace this visitor with a deep copy.
    RelOperator[] physicalOperators = operator.acceptVisitor(physicalOperatorGenerator);

    for (RelOperator physicalOperator : physicalOperators) {
      if (!operator.hasChildren()) {
        continue;
      }
      // traversal children
      Task[] tasks = new Task[operator.getChildren().length];
      for (int i = 0; i < tasks.length; i++) {
        RelOperator child = operator.getChildren()[i];
        tasks[i] = child.acceptVisitor(session, this, physicalOperator.getPhysicalProperty());
      }
      Task tempTask = physicalOperator.acceptVisitor(session, operatorToTaskAttacher, tasks);
      bestTask = getTaskWithLowestCost(bestTask, tempTask);
    }

    return bestTask;
  }

  @Override
  public Task visitOperator(Session session, TableSource tableSource, PhysicalProperty prop) {
    Task bestTask = ProxyTask.initProxyTask();
    List<PathCandidate> candidates = pruneAccessPaths(tableSource);

    for (PathCandidate candidate : candidates) {
      TableAccessPath path = candidate.path;
      if (path.isTablePath()) {
        Task task = buildTableSourceTask(tableSource, path, prop);
        bestTask = getTaskWithLowestCost(task, bestTask);
        continue;
      }

      Task indexTask = buildIndexSourceTask(session, tableSource, path, prop);
      bestTask = getTaskWithLowestCost(indexTask, bestTask);
    }
    return bestTask;
  }

  @Override
  public Task visitOperator(Session session, DualTable dualTable, PhysicalProperty prop) {
    Cost cost = new Cost(dualTable.getRowCount());
    ProxyTask proxyTask = new ProxyTask(cost);
    proxyTask.setPlan(dualTable);
    return proxyTask;
  }

  private Task getTaskWithLowestCost(Task first, Task second) {
    double firstCost = first.getCost();
    if (first instanceof DSTask) {
      if (((DSTask) first).getIndexSource() != null) {
        firstCost += first.getStatRowCount() * StatsUtils.NETWORK_FACTOR;
      }
    }

    double secondCost = second.getCost();
    if (second instanceof DSTask) {
      if (((DSTask) second).getIndexSource() != null) {
        secondCost += second.getStatRowCount() * StatsUtils.NETWORK_FACTOR;
      }
    }

    return firstCost < secondCost ? first : second;
  }

  private List<PathCandidate> pruneAccessPaths(TableSource tableSource) {
    LinkedList<PathCandidate> candidates = Lists.newLinkedList();
    for (TableAccessPath path : tableSource.getTableAccessPaths()) {
      PathCandidate currentCandidate = new PathCandidate(path);
      if (path.getRanges() == null || path.getRanges().isEmpty()) {
        return Collections.singletonList(currentCandidate);
      }

      if (path.isTablePath()) {
        // get table candidates
        path.setSingleScan(true);
        currentCandidate.columnIdSet = extractColumnIds(path.getAccessConditions());
      } else if (path.getAccessConditions() != null && !path.getAccessConditions().isEmpty()) {
        // get index candidates
        boolean isSingleScan = isSingleScan(tableSource.getSchema().getColumns(), path.getIndexColumns(),
                tableSource.getTable().getPrimary()[0]);
        path.setSingleScan(isSingleScan);
        currentCandidate.columnIdSet = extractColumnIds(path.getAccessConditions());
      } else {
        continue;
      }

      boolean shouldBePruned = false;

      // iterator the candidates in reverse order. Note that the candidates are sorted in ascending order based on
      // the 'cost'
      Iterator<PathCandidate> it = candidates.descendingIterator();
      while (it.hasNext()) {
        PathCandidate candidate = it.next();
        int result = comparePathCandidates(candidate, currentCandidate);
        if (result < 0) {
          // if cost(candidate) < cost(currentCandidate); we cannot prune other candidates since the candidates
          // are sorted in ascending order based on the 'cost', and the most expensive candidate in the list is
          // cheaper than 'currentCandidate'
          shouldBePruned = true; // discard currentCandidate
          break;
        } else if (result > 0) {
          it.remove(); // discard candidate
        }
      }

      if (!shouldBePruned) {
        candidates.add(currentCandidate);
      }
    }
    return candidates;
  }

  private boolean isSingleScan(List<ColumnExpr> columns, List<ColumnExpr> indexColumns, Column primaryKey) {
    for (ColumnExpr columnExpr : columns) {
      if (primaryKey != null && columnExpr.getId().equals(primaryKey.getId())) {
        continue;
      }

      boolean isIndexColumn = false;
      for (ColumnExpr indexCol : indexColumns) {
        if (columnExpr.getUid().equals(indexCol.getUid())) {
          isIndexColumn = true;
          break;
        }
      }

      if (!isIndexColumn) {
        return false;
      }
    }
    return true;
  }

  // Compare the two path candidates
  private int comparePathCandidates(PathCandidate oldPath, PathCandidate newPath) {
    int cost = 0;
    if (oldPath.path.isSingleScan() && !newPath.path.isSingleScan()) {
      cost--;
    } else if (!oldPath.path.isSingleScan() && newPath.path.isSingleScan()) {
      cost++;
    }

    if (oldPath.columnIdSet.size() > newPath.columnIdSet.size()
            && oldPath.columnIdSet.containsAll(newPath.columnIdSet)) {
      cost--;
    } else if (newPath.columnIdSet.size() > oldPath.columnIdSet.size()
            && newPath.columnIdSet.containsAll(oldPath.columnIdSet)) {
      cost++;
    }

    return cost;
  }

  private Task buildTableSourceTask(TableSource tableSource, TableAccessPath accessPath,
                                    PhysicalProperty prop) {
    if (!prop.isEmpty()) {
      Optional<Column> pkCol = ExpressionUtil.getPkCol(tableSource.getColumns());
      if (!(prop.entries.length == 1 && pkCol.isPresent() && pkCol.get().getName().equals(prop.entries[0].columnExpr.getOriCol()))) {
        return ProxyTask.initProxyTask();
      }
    }
    // FIXME is this correct?
    TableSource physicalTableSource = (TableSource) tableSource.acceptVisitor(physicalOperatorGenerator)[0];
    Task task = new DSTask(physicalTableSource);

    final KeyValueRange range = new KeyValueRange(accessPath.getIndex(), accessPath.getIndexColumns(),
            accessPath.getRanges());
    physicalTableSource.setKeyValueRange(range);
    physicalTableSource.setStatInfo(new OperatorStatsInfo(accessPath.getCountOnAccess()));

    // FIXME remove this redundancy
    physicalTableSource.setAccessConditions(accessPath.getAccessConditions());

    double cost = accessPath.getCountOnAccess() * StatsUtils.TABLE_SCAN_FACTOR;

    if (accessPath.getTableConditions() != null && !accessPath.getTableConditions().isEmpty()) {
      cost += task.getStatRowCount() * StatsUtils.CPU_FACTOR;

      // TODO we need to scale the count based on the expected count in the physical property
      final Selection selection = new Selection(accessPath.getTableConditions(), tableSource.getStatInfo());
      ((DSTask) task).attachOperatorToPushedDownTablePlan(selection);
    }

    // for PROXY tasks, we need finish the task here
    if (prop.taskType == Task.TaskType.PROXY) {
      task = task.finish();
    }

    if (cost != 0) {
      task.addCost(cost);
      return task;
    }

    LOG.debug("Could not calculate cost for TableSourceTask whose CountOnAccess = {} and StatRowCount = {}",
            accessPath.getCountOnAccess(), task.getStatRowCount());

    if (range.isFullRange()) {
      task.cost = Cost.newFullRangeCost();
    } else {
      task.cost = Cost.newIndexPlanCost();
    }

    // TODO uncomment setCost when all tests passed
    //task.setCost(cost);
    return task;
  }

  private boolean matchIndicesProp(List<ColumnExpr> idxColumnExprs, PhysicalProperty.Entry[] propEntries) {
    if (idxColumnExprs.size() < propEntries.length) {
      return false;
    }
    for (int i = 0; i < propEntries.length; i++) {
      if (!idxColumnExprs.get(i).getUid().equals(propEntries[i].columnExpr.getUid())) {
        return false;
      }
    }
    return true;
  }

  private Task buildIndexSourceTask(Session session, TableSource tableSource, TableAccessPath accessPath,
                                    PhysicalProperty prop) {
    if (!prop.isEmpty()) {
      List<ColumnExpr> idxColumnExprs = accessPath.getIndexColumns();
      for (int i = 0; i < idxColumnExprs.size(); i++) {
        if (idxColumnExprs.get(i).getUid().equals(prop.entries[0].columnExpr.getUid())) {
          boolean isMatchProp = matchIndicesProp(idxColumnExprs.subList(i, idxColumnExprs.size()), prop.entries);
          if (!isMatchProp) {
            return ProxyTask.initProxyTask();
          }
        }
      }
    }
    final KeyValueRange range = new KeyValueRange(accessPath.getIndex(), accessPath.getIndexColumns(),
            accessPath.getRanges());
    final OperatorStatsInfo statInfo = new OperatorStatsInfo(accessPath.getCountOnAccess());

    final Schema schema = initSchema(session, tableSource.getSchema(), tableSource.getDbName(),
            tableSource.getTable(), tableSource.getColumns(), accessPath);

    final IndexSource indexSource = new IndexSource(tableSource.getTable(), range, schema, statInfo);

    // FIXME remove this redundancy, we do not need to have this access condition stored in this way
    indexSource.setAccessConditions(accessPath.getAccessConditions());

    Task task = new DSTask(indexSource);
    double cost = accessPath.getCountOnAccess() * StatsUtils.TABLE_SCAN_FACTOR;

    if (!accessPath.isSingleScan()) {
      TableSource newTableSource = tableSource.copy();
      //task.attachOperator(newTableSource);
      ((DSTask) task).setTableSource(newTableSource);
      ((DSTask) task).attachOperatorToPushedDownTablePlan(newTableSource);
    }

    if (accessPath.getIndexConditions() != null && !accessPath.getIndexConditions().isEmpty()) {
      cost += cost * StatsUtils.CPU_FACTOR;

      double selectivity = 0;
      if (accessPath.getCountOnAccess() > 0) {
        selectivity = accessPath.getCountOnIndex() / accessPath.getCountOnAccess();
      }

      final double rowCount = indexSource.getStatInfo().getEstimatedRowCount() * selectivity;

      final Selection selection = new Selection(accessPath.getIndexConditions(), new OperatorStatsInfo(rowCount));
      ((DSTask) task).attachOperatorToPushedDownIndexPlan(selection);
    }

    if (accessPath.getTableConditions() != null && !accessPath.getTableConditions().isEmpty()) {
      ((DSTask) task).finishIndexPlan();
      // TODO we need to scale the count based on the expected count in the physical property
      final Selection selection = new Selection(accessPath.getTableConditions(), tableSource.getStatInfo());
      ((DSTask) task).attachOperatorToPushedDownTablePlan(selection);
    }

    // for PROXY tasks, we need finish the task here
    if (prop.taskType == Task.TaskType.PROXY) {
      task = task.finish();
    }

    if (cost != 0) {
      task.addCost(cost);
      return task;
    }

    LOG.debug("Could not calculate cost for IndexSourceTask whose CountOnIndex = {}, CountOnAccess = {}, and "
                    + "StatRowCount = {}",
            accessPath.getCountOnIndex(), accessPath.getCountOnAccess(),
            indexSource.getStatInfo().getEstimatedRowCount());

    if (range.isFullRange()) {
      task.cost = Cost.newFullRangeCost();
    } else {
      task.cost = Cost.newIndexPlanCost();
    }

    return task;
  }

  /**
   * Initialize IndexSource schema according to TableSource(logical).columns, index and pk.
   * The schema columns contain index column(s) and pk (pk will be added when double read or pk is in projection)
   */
  private Schema initSchema(Session session, Schema schema, String dbName, Table table, Column[] columns,
                            TableAccessPath accessPath) {
    // get index columns
    List<ColumnExpr> result = new ArrayList<>(columns.length);
    Schema newSchema = new Schema(result);

    if (accessPath.isSingleScan()) {
      for (int i = 0; i < columns.length; i++) {
        // if columns of tableSource contains pk, then put it to result
        if (columns[i].isPrimary()) {
          result.add(schema.getColumn(i));
        }
      }
    } else {
      // double read, get primary columns
      List<ColumnExpr> pkList = ExpressionUtil.buildColumnExpr(session, dbName,
              table.getName(), table.getPrimary());
      result.addAll(pkList);
    }

    Column[] indexCols = accessPath.getIndex().getColumns();
    for (Column indexCol : indexCols) {
      for (ColumnExpr columnExpr : schema.getColumns()) {
        if (indexCol.getId().longValue() == columnExpr.getId().longValue()) {
          result.add(columnExpr);
        }
      }
    }

    return newSchema;
  }

  /**
   * PathCandidate is used to help select good path candidate
   */
  private static class PathCandidate {
    TableAccessPath path;
    Set<Long> columnIdSet = new HashSet<>();

    PathCandidate(TableAccessPath path) {
      this.path = path;
    }
  }
}
