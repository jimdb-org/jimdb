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
package io.jimdb.sql.smith.cxt;


import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Smither;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * @version V1.0
 */
public final class Scope {

  private Smither smither;

  int budget;

  public Scope(MetaEngine engine, String catalog, int budget) {
    smither = new Smither();
    this.budget = budget;
    Catalog database = engine.getMetaData().getCatalog(catalog);
    smither.setTables(database.getTables());
  }

  public boolean canRecurse() {
    this.budget--;
    return this.budget > 0 && Random.coin();
  }

  public SQLStatement makeSQLStmt() {
    int index = smither.getStatementWeight().getNext();
    return (SQLStatement) smither.getStatementWeights()[index].makeStatement(this);
  }

  public Smither getSmither() {
    return smither;
  }

  public void setSmither(Smither smither) {
    this.smither = smither;
  }

  public int getBudget() {
    return budget;
  }

  public void setBudget(int budget) {
    this.budget = budget;
  }
}
