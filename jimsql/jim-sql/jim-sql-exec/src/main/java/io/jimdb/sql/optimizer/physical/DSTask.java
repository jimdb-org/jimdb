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

import java.util.Objects;

import io.jimdb.sql.operator.IndexLookup;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.optimizer.statistics.StatsUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A DataSource (DS) task records the operators to be pushed down to data-server.
 * Note that a DS task may contain both table and index plans.
 */
@SuppressFBWarnings()
public class DSTask extends Task {

  // pushed down table plan tree, the leaf node is tableSource
  private RelOperator pushedDownTablePlan;

  // pushed down index plan tree, the leaf node is indexSource
  private RelOperator pushedDownIndexPlan;

  // record the leaf of tree
  private TableSource tableSource;

  // record the leaf of tree
  private IndexSource indexSource;

  private boolean isIndexFinished = false;

  public DSTask(TableSource operator) {
    this.pushedDownTablePlan = operator;
    this.tableSource = operator;
    this.isIndexFinished = true;
    this.cost = new Cost(0);
  }

  public DSTask(IndexSource operator) {
    this.pushedDownIndexPlan = operator;
    this.indexSource = operator;
    this.cost = new Cost(0);
  }

  @Override
  public RelOperator getPlan() {
    return isIndexFinished ? pushedDownTablePlan : pushedDownIndexPlan;
  }

  // Close the DS task and create a proxy task
  @Override
  public Task finish() {
    this.finishIndexPlan();

    ProxyTask proxyTask = new ProxyTask(this.cost);
    if (this.pushedDownTablePlan != null && this.pushedDownIndexPlan != null) {
      IndexLookup indexLookup = new IndexLookup(this);
      Objects.requireNonNull(indexLookup.getStatInfo());
      proxyTask.setPlan(indexLookup);
    } else if (this.pushedDownTablePlan != null) {
      TableSource tableSource = new TableSource(this.tableSource, this.pushedDownTablePlan);
      Objects.requireNonNull(tableSource.getStatInfo());
      proxyTask.setPlan(tableSource);
    } else {
      IndexSource indexSource = new IndexSource(this.indexSource, this.pushedDownIndexPlan);
      Objects.requireNonNull(indexSource.getStatInfo());
      proxyTask.setPlan(indexSource);
    }
    return proxyTask;
  }

  @Override
  public void attachOperator(RelOperator operator) {
    if (this.isIndexFinished) {
      operator.setChildren(this.pushedDownTablePlan);
      this.pushedDownTablePlan = operator;
    } else {
      operator.setChildren(this.pushedDownIndexPlan);
      this.pushedDownIndexPlan = operator;
    }
  }

  public void attachOperatorToPushedDownIndexPlan(RelOperator operator) {
    if (this.pushedDownIndexPlan != null) {
      operator.setChildren(this.pushedDownIndexPlan);
    }
    this.pushedDownIndexPlan = operator;
  }

  public void attachOperatorToPushedDownTablePlan(RelOperator operator) {
    if (this.pushedDownTablePlan != null) {
      operator.setChildren(this.pushedDownTablePlan);
    }
    this.pushedDownTablePlan = operator;
  }

  public void finishIndexPlan() {
    if (isIndexFinished) {
      return;
    }

    double costWithNetFactor = getStatRowCount() * StatsUtils.NETWORK_FACTOR;
    cost.plus(costWithNetFactor);

    isIndexFinished = true;
    if (pushedDownTablePlan != null) {
      pushedDownTablePlan.setStatInfo(pushedDownIndexPlan.getStatInfo());
      double costWithScanFactor = getStatRowCount() * StatsUtils.TABLE_SCAN_FACTOR;
      cost.plus(costWithScanFactor);
    }
  }

  public void setTableSource(TableSource tableSource) {
    this.tableSource = tableSource;
  }

  public void setIndexSource(IndexSource indexSource) {
    this.indexSource = indexSource;
  }

  public RelOperator getPushedDownTablePlan() {
    return this.pushedDownTablePlan;
  }

  public RelOperator getPushedDownIndexPlan() {
    return this.pushedDownIndexPlan;
  }

  public TableSource getTableSource() {
    return tableSource;
  }

  public IndexSource getIndexSource() {
    return indexSource;
  }

  public boolean isIndexFinished() {
    return isIndexFinished;
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.DS;
  }

  @Override
  public double getStatRowCount() {
    if (isIndexFinished && null != pushedDownTablePlan) {
      return pushedDownTablePlan.getStatInfo().getEstimatedRowCount();
    }
    if (null != pushedDownIndexPlan) {
      return pushedDownIndexPlan.getStatInfo().getEstimatedRowCount();
    }
    return tableSource.getStatInfo().getEstimatedRowCount();
  }
}
