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
package io.jimdb.sql.gen.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class Table {
  private String name;
  private int replica;
  private int partition;
  private Integer[] rows;
  private String[] engines;
  private Field[] fields;
  private Index[] indices;

  public Table() {
    this.replica = 1;
    this.partition = 1;
    this.rows = new Integer[]{ 10000 };
    this.engines = new String[]{ "memory" };
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getReplica() {
    return replica;
  }

  public void setReplica(int replica) {
    this.replica = replica;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public Integer[] getRows() {
    return rows;
  }

  public void setRows(Integer[] rows) {
    this.rows = rows;
  }

  public String[] getEngines() {
    return engines;
  }

  public void setEngines(String[] engines) {
    this.engines = engines;
  }

  public Field[] getFields() {
    return fields;
  }

  public void setFields(Field[] fields) {
    this.fields = fields;
  }

  public Index[] getIndices() {
    return indices;
  }

  public void setIndices(Index[] indices) {
    this.indices = indices;
  }
}
