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
import io.jimdb.sql.optimizer.statistics.OperatorStatsInfo;

/**
 * The operator list of Proxy Task will be executed in proxy
 */
public class ProxyTask extends Task {
  RelOperator tablePlan;

  public ProxyTask(Cost cost) {
    this.cost = cost;
  }

  public void setPlan(RelOperator operator) {
    this.tablePlan = operator;
  }

  @Override
  public RelOperator getPlan() {
    return this.tablePlan;
  }

  @Override
  public Task finish() {
    return this;
  }

  @Override
  public void attachOperator(RelOperator operator) {
    operator.setChildren(this.tablePlan);
    this.tablePlan = operator;
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.PROXY;
  }

  public static ProxyTask initProxyTask() {
    return new ProxyTask(Cost.newMaxCost());
  }

  @Override
  public double getStatRowCount() {
    OperatorStatsInfo statInfo = tablePlan.getStatInfo();
    return statInfo.getEstimatedRowCount();
  }
}
