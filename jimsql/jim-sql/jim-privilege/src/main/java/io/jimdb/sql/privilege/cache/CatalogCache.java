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
package io.jimdb.sql.privilege.cache;

/**
 * @version V1.0
 */
public final class CatalogCache {
  private final String user;
  private final String host;
  private final String catalog;
  private final int privilege;

  public CatalogCache(String user, String host, String catalog, int privilege) {
    this.user = user;
    this.host = host;
    this.catalog = catalog;
    this.privilege = privilege;
  }

  public String getUser() {
    return user;
  }

  public String getHost() {
    return host;
  }

  public String getCatalog() {
    return catalog;
  }

  public int getPrivilege() {
    return privilege;
  }
}
