/*
 * Copyright 2019 The Chubao Authors.
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
package io.jimdb.sql.smith;

import io.jimdb.model.Catalog;
import io.jimdb.model.Table;
import io.jimdb.sql.smith.scalar.Scalar;
import io.jimdb.sql.smith.stmt.SelectStatement;
import io.jimdb.sql.smith.stmt.StatementWeight;
import io.jimdb.sql.smith.table.SimpleTableSourceExpr;
import io.jimdb.sql.smith.table.TableExprWeights;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY"})
public final class Smither {

  private Catalog sourceSchema;
  private Table[] tables;

  private WeightedSampler selectWeight;

  private WeightedSampler statementWeight;
  private WeightedSampler tableExprWeight;

  private StatementWeight[] statementWeights;
  private TableExprWeights[] tableExprWeights;

  private SQLOrderingSpecification[] orderDirections = { SQLOrderingSpecification.ASC, SQLOrderingSpecification.DESC };

  private boolean avoidConsts;
  private boolean disableLimits;

  private long nameConut = 0L;

  public Smither() {
    Scalar scalar = new Scalar();
    statementWeights = new StatementWeight[]{
            new SelectStatement(scalar, 10)
    };

    tableExprWeights = new TableExprWeights[]{
            new SimpleTableSourceExpr(10)
    };

    int[] stmtWeights = new int[statementWeights.length];
    for (int i = 0; i < statementWeights.length; i++) {
      stmtWeights[i] = statementWeights[i].getWeight();
    }
    statementWeight = new WeightedSampler(stmtWeights, Random.rand100());

    int[] tableWeights = new int[tableExprWeights.length];
    for (int i = 0; i < tableExprWeights.length; i++) {
      tableWeights[i] = tableExprWeights[i].getWeight();
    }
    tableExprWeight = new WeightedSampler(tableWeights, Random.rand100());
  }

  public Table getRandomTable() {

    //TODO debug
    for (Table table : tables) {
      if ("student".equals(table.getName())) {
        return table;
      }
    }
    return tables[Random.getRandomInt(tables.length)];
  }

  public Catalog getSourceSchema() {
    return sourceSchema;
  }

  public void setSourceSchema(Catalog sourceSchema) {
    this.sourceSchema = sourceSchema;
  }

  public Table[] getTables() {
    return tables;
  }

  public void setTables(Table[] tables) {
    this.tables = tables;
  }

  public int getRandomInt(int i) {
    return Random.getRandomInt(i);
  }

  public boolean isAvoidConsts() {
    return avoidConsts;
  }

  public void setAvoidConsts(boolean avoidConsts) {
    this.avoidConsts = avoidConsts;
  }

  public WeightedSampler getSelectWeight() {
    return selectWeight;
  }

  public void setSelectWeight(WeightedSampler selectWeight) {
    this.selectWeight = selectWeight;
  }

  public StatementWeight[] getStatementWeights() {
    return statementWeights;
  }

  public void setStatementWeights(StatementWeight[] statementWeights) {
    this.statementWeights = statementWeights;
  }

  public TableExprWeights[] getTableExprWeights() {
    return tableExprWeights;
  }

  public void setTableExprWeights(TableExprWeights[] tableExprWeights) {
    this.tableExprWeights = tableExprWeights;
  }

  public WeightedSampler getStatementWeight() {
    return statementWeight;
  }

  public String getName(String prefix) {
    nameConut++;
    return String.format("%s_%d", prefix, nameConut);
  }

  public SQLOrderingSpecification randDirection() {
    return orderDirections[Random.getRandomInt(orderDirections.length)];
  }

  public boolean isDisableLimits() {
    return disableLimits;
  }

  public void setDisableLimits(boolean disableLimits) {
    this.disableLimits = disableLimits;
  }

  public void setStatementWeight(WeightedSampler statementWeight) {
    this.statementWeight = statementWeight;
  }

  public WeightedSampler getTableExprWeight() {
    return tableExprWeight;
  }

  public void setTableExprWeight(WeightedSampler tableExprWeight) {
    this.tableExprWeight = tableExprWeight;
  }
}
