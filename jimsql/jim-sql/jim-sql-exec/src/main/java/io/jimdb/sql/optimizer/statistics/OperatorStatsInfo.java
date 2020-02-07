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
package io.jimdb.sql.optimizer.statistics;

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * OperatorStatsInfo is used to store statistics information for operators.
 */
@SuppressFBWarnings()
public class OperatorStatsInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorStatsInfo.class);

  // estimated total row count in the table, note that this is different from the row count in table stats
  double estimatedRowCount;

  // number of distinct values in each column
  double[] cardinalityList;

  public OperatorStatsInfo(double estimatedRowCount) {
    this.estimatedRowCount = estimatedRowCount;
    this.cardinalityList = new double[0];
  }

  public OperatorStatsInfo(double estimatedRowCount, double[] cardinalityList) {
    this.estimatedRowCount = estimatedRowCount;
    this.cardinalityList = cardinalityList;
  }

  public OperatorStatsInfo adjust(double selectivity) {
    for (int i = 0; i < cardinalityList.length; i++) {
      cardinalityList[i] = cardinalityList[i] * selectivity;
    }
    estimatedRowCount = estimatedRowCount * selectivity;
    return new OperatorStatsInfo(estimatedRowCount, cardinalityList);
  }

  public double getEstimatedRowCount() {
    return estimatedRowCount;
  }

  public void setEstimatedRowCount(double estimatedRowCount) {
    this.estimatedRowCount = estimatedRowCount;
  }

  public int getCardinalityListSize() {
    return cardinalityList.length;
  }

  public double getCardinality(int index) {
    if (index >= cardinalityList.length) {
      LOG.debug("index {} is out of array cardinalityList's bound {}", index, cardinalityList.length);
      return 0;
    }

    return cardinalityList[index];
  }

  public double[] newCardinalityList(double rowCount) {
    double[] cardinality = new double[this.cardinalityList.length];
    for (int i = 0; i < cardinality.length; i++) {
      cardinality[i] = Math.min(this.cardinalityList[i], rowCount);
    }
    return cardinality;
  }

  public double estimateRowCountByColumnRanges(Session session, long uid, List<ValueRange> ranges) {
    return 0;
  }

  public double estimateRowCountByIndexedRanges(Session session, int indexId, List<ValueRange> ranges) {
    return 0;
  }

  public double calculateSelectivity(Session session, List<Expression> expressionList) {
    return 0;
  }
}
