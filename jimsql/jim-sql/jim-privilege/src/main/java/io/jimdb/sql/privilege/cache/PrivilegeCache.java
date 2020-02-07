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
package io.jimdb.sql.privilege.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.utils.lang.StringUtil;

/**
 * @version V1.0
 */
public final class PrivilegeCache {
  private final Map<String, List<UserCache>> userPrivileges;
  private final Map<String, List<CatalogCache>> catalogPrivileges;
  private final Map<String, List<TableCache>> tablePrivileges;
  private final long version;

  public PrivilegeCache(List<UserCache> users, List<CatalogCache> catalogs, List<TableCache> tables, long version) {
    this.version = version;
    this.userPrivileges = new HashMap<>();
    this.catalogPrivileges = new HashMap<>();
    this.tablePrivileges = new HashMap<>();

    this.loadUserPrivilege(users);
    this.loadCatalogPrivilege(catalogs);
    this.loadTablePrivilege(tables);
  }

  public long getVersion() {
    return version;
  }

  public List<UserCache> getUserPrivileges() {
    List<UserCache> result = new ArrayList<>();
    userPrivileges.forEach((k, v) -> result.addAll(v));
    return result;
  }

  public List<CatalogCache> getCatalogPrivileges() {
    List<CatalogCache> result = new ArrayList<>();
    catalogPrivileges.forEach((k, v) -> result.addAll(v));
    return result;
  }

  public List<TableCache> getTablePrivileges() {
    List<TableCache> result = new ArrayList<>();
    tablePrivileges.forEach((k, v) -> result.addAll(v));
    return result;
  }

  public UserCache matchUser(String user, String host) {
    List<UserCache> userCaches = userPrivileges.get(user.toLowerCase());
    if (userCaches != null) {
      for (UserCache userRecord : userCaches) {
        if (StringUtil.matchString(host, userRecord.getHost())) {
          return userRecord;
        }
      }
    }

    return null;
  }

  public CatalogCache matchCatalog(String user, String host, String catalog) {
    List<CatalogCache> catalogCaches = catalogPrivileges.get(user.toLowerCase());
    if (catalogCaches != null) {
      for (CatalogCache catalogRecord : catalogCaches) {
        if (StringUtil.matchString(host, catalogRecord.getHost()) && StringUtil.matchCatalogString(catalog, catalogRecord.getCatalog())) {
          return catalogRecord;
        }
      }
    }

    return null;
  }

  public TableCache matchTable(String user, String host, String catalog) {
    List<TableCache> tableCaches = tablePrivileges.get(user.toLowerCase());
    if (tableCaches != null) {
      for (TableCache tableRecord : tableCaches) {
        if (StringUtil.matchString(host, tableRecord.getHost()) && StringUtil.matchString(catalog, tableRecord.getCatalog())) {
          return tableRecord;
        }
      }
    }

    return null;
  }

  public TableCache matchTable(String user, String host, String catalog, String table) {
    List<TableCache> tableCaches = tablePrivileges.get(user.toLowerCase());
    if (tableCaches != null) {
      for (TableCache tableRecord : tableCaches) {
        if (StringUtil.matchString(host, tableRecord.getHost()) && StringUtil.matchString(catalog, tableRecord.getCatalog())
                && tableRecord.getTable().equalsIgnoreCase(table)) {
          return tableRecord;
        }
      }
    }

    return null;
  }

  private void loadUserPrivilege(List<UserCache> users) {
    for (UserCache user : users) {
      List<UserCache> userCaches = userPrivileges.get(user.getUser().toLowerCase());
      if (userCaches == null) {
        userCaches = new ArrayList<>();
      }
      userCaches.add(user);
      userPrivileges.put(user.getUser().toLowerCase(), userCaches);
    }
  }

  private void loadCatalogPrivilege(List<CatalogCache> catalogs) {
    for (CatalogCache catalog : catalogs) {
      List<CatalogCache> catalogCaches = catalogPrivileges.get(catalog.getUser().toLowerCase());
      if (catalogCaches == null) {
        catalogCaches = new ArrayList<>();
      }
      catalogCaches.add(catalog);
      catalogPrivileges.put(catalog.getUser().toLowerCase(), catalogCaches);
    }
  }

  private void loadTablePrivilege(List<TableCache> tables) {
    for (TableCache table : tables) {
      List<TableCache> tableCaches = tablePrivileges.get(table.getUser().toLowerCase());
      if (tableCaches == null) {
        tableCaches = new ArrayList<>();
      }
      tableCaches.add(table);
      tablePrivileges.put(table.getUser().toLowerCase(), tableCaches);
    }
  }
}
