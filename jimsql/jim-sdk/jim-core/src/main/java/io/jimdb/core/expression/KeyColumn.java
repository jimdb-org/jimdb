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
package io.jimdb.core.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * IndexColumn
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class KeyColumn implements Cloneable {
  // columns that are unique/primary index(es)
  private List<ColumnExpr> columnExprs;

  public KeyColumn(int size) {
    this.columnExprs = new ArrayList<>(size);
  }

  public KeyColumn(ColumnExpr... columnExprs) {
    this.columnExprs = new ArrayList<>(columnExprs.length);
    Collections.addAll(this.columnExprs, columnExprs);
  }

  public KeyColumn(List<ColumnExpr> columnExprs) {
    this.columnExprs = columnExprs;
  }

  @Override
  protected KeyColumn clone() {
    List<ColumnExpr> result = new ArrayList<>(columnExprs.size());
    for (ColumnExpr column : columnExprs) {
      result.add(column.clone());
    }
    return new KeyColumn(result);
  }

  public List<ColumnExpr> getColumnExprs() {
    return this.columnExprs;
  }

  public void addColumnExpr(ColumnExpr columnExpr) {
    if (this.columnExprs == null) {
      this.columnExprs = new ArrayList<>();
    }
    if (columnExpr != null) {
      this.columnExprs.add(columnExpr);
    }
  }
}
