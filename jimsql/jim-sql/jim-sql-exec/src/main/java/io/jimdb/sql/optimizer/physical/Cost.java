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

/**
 * Cost
 */
public class Cost {

  private double cost;

  public Cost(double rowCount) {
    this.cost = rowCount;
  }

  public Cost plus(double toAdd) {
    this.cost += toAdd;
    return this;
  }

  public Cost multiplyFactorAndPlus(double factor) {
    cost += cost * factor;
    return this;
  }

  public double getCost() {
    return cost;
  }

  public static Cost newMaxCost() {
    return new Cost(Long.MAX_VALUE);
  }

  public static Cost newFullRangeCost() {
    return new Cost(Integer.MAX_VALUE);
  }

  public static Cost newTablePlanCost() {
    return new Cost(1);
  }

  public static Cost newIndexPlanCost() {
    return new Cost(2);
  }
}
