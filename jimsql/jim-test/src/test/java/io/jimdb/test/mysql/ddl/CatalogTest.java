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
package io.jimdb.test.mysql.ddl;

import java.sql.SQLException;
import java.util.List;

import io.jimdb.core.config.JimConfig;
import io.jimdb.meta.EtcdMetaStore;
import io.jimdb.pb.Metapb;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.test.mysql.SqlTestBase;
import io.jimdb.common.utils.lang.IOUtil;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class CatalogTest extends SqlTestBase {
  private static final String OVER_NAME;
  private static final MetaStore metaStore;

  static {
    StringBuilder builder = new StringBuilder(128);
    for (int i = 0; i < 128; i++) {
      builder.append("a");
    }
    OVER_NAME = builder.toString();

    metaStore = new EtcdMetaStore();
    metaStore.init(new JimConfig(IOUtil.loadResource("jim_test.properties")));
  }

  @Test
  public void testDropError() {
    String sql = "Drop database 'test@'";
    SQLException result = new SQLException("Incorrect database name 'test@'", "42000", 1102);
    execUpdate(sql, result, true);

    sql = String.format("Drop database %s", OVER_NAME);
    result = new SQLException(String.format("Identifier name '%s' is too long", OVER_NAME), "42000", 1059);
    execUpdate(sql, result, true);

    sql = "Drop database 'null'";
    result = new SQLException("Unknown database 'null'", "42000", 1049);
    execUpdate(sql, result, true);
  }

  @Test
  public void testDropWithExists() {
    String sql = "Drop database IF EXISTS 'null'";
    execUpdate(sql, 0, true);
  }

  @Test
  public void testCreateNameError() {
    String sql = "Create database 'test@'";
    SQLException result = new SQLException("Incorrect database name 'test@'", "42000", 1102);
    execUpdate(sql, result, true);

    sql = String.format("Create database %s", OVER_NAME);
    result = new SQLException(String.format("Identifier name '%s' is too long", OVER_NAME), "42000", 1059);
    execUpdate(sql, result, true);
  }

  @Test
  public void testCreate() {
    String name = "test" + System.nanoTime();
    deleteCatalog(name);

    List<Metapb.CatalogInfo> catalogs = metaStore.getCatalogs();
    for (Metapb.CatalogInfo catalog : catalogs) {
      Assert.assertNotEquals(name.toLowerCase(), catalog.getName().toLowerCase());
    }

    String sql = String.format("Create database %s", name);
    execUpdate(sql, 0, true);

    boolean exist = false;
    catalogs = metaStore.getCatalogs();
    for (Metapb.CatalogInfo catalog : catalogs) {
      if (name.equalsIgnoreCase(catalog.getName())) {
        exist = true;
        break;
      }
    }
    Assert.assertEquals(true, exist);

    deleteCatalog(name);
  }

  @Test
  public void testCreateDup() {
    String name = "test" + System.nanoTime();
    deleteCatalog(name);

    List<Metapb.CatalogInfo> catalogs = metaStore.getCatalogs();
    for (Metapb.CatalogInfo catalog : catalogs) {
      Assert.assertNotEquals(name.toLowerCase(), catalog.getName().toLowerCase());
    }

    String sql = String.format("Create database %s", name);
    execUpdate(sql, 0, true);

    boolean exist = false;
    catalogs = metaStore.getCatalogs();
    for (Metapb.CatalogInfo catalog : catalogs) {
      if (name.equalsIgnoreCase(catalog.getName())) {
        exist = true;
        break;
      }
    }
    Assert.assertEquals(true, exist);

    sql = String.format("Create database %s", name);
    SQLException result = new SQLException(String.format("Can't create database '%s'; database exists", name), "HY000", 1007);
    execUpdate(sql, result, true);

    sql = String.format("Create database IF NOT EXISTS %s", name);
    execUpdate(sql, 0, true);

    deleteCatalog(name);
  }

  @Test
  public void testUsedb() {
    String name = "test" + System.nanoTime();
    deleteCatalog(name);

    List<Metapb.CatalogInfo> catalogs = metaStore.getCatalogs();
    for (Metapb.CatalogInfo catalog : catalogs) {
      Assert.assertNotEquals(name.toLowerCase(), catalog.getName().toLowerCase());
    }

    String sql = String.format("use %s", name);
    SQLException result = new SQLException(String.format("Unknown database '%s'", name), "42000", 1049);
    execUpdate(sql, result, true);

    sql = String.format("Create database %s", name);
    execUpdate(sql, 0, true);

    sql = String.format("use %s", name);
    execUpdate(sql, 0, true);

    deleteCatalog(name);
  }
}
