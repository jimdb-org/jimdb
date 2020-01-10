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

import io.jimdb.sql.operator.RelOperator;

import com.google.common.annotations.VisibleForTesting;

/**
 * Task is used for CBO and to build physical plan tree.
 * The type of task is ProxyTask and DSTask
 */
public abstract class Task {

  Cost cost;

  public abstract RelOperator getPlan();

  public abstract Task finish();

  public abstract void attachOperator(RelOperator operator);

  @VisibleForTesting
  public double getCost() {
    return cost.getCost();
  }

  Cost addCost(double cost) {
    return this.cost.plus(cost);
  }

  public abstract TaskType getTaskType();

  public abstract double getStatRowCount();

  /**
   * task type: ProxyTask, DSTask
   */
  public enum TaskType {
    PROXY, DS
  }
}
