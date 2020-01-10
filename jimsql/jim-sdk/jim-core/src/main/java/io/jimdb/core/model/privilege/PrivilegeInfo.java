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
package io.jimdb.core.model.privilege;

/**
 * @version V1.0
 */
public final class PrivilegeInfo {
  private final String catalog;
  private final String table;
  private final String column;
  private final PrivilegeType type;

  public PrivilegeInfo(String catalog, String table, PrivilegeType type) {
    this(catalog, table, null, type);
  }

  public PrivilegeInfo(String catalog, String table, String column, PrivilegeType type) {
    this.catalog = catalog;
    this.table = table;
    this.column = column;
    this.type = type;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getTable() {
    return table;
  }

  public String getColumn() {
    return column;
  }

  public PrivilegeType getType() {
    return type;
  }
}
