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
package io.jimdb.core.model.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public final class MetaData {
  private static final Logger LOG = LoggerFactory.getLogger(MetaData.class);

  private static final AtomicIntegerFieldUpdater<MetaData> REFCNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MetaData.class, "refCnt");

  private final long version;
  private final Map<String, Catalog> catalogMap;
  private final List<Catalog> catalogs;
  private volatile int refCnt;

  public MetaData(long version) {
    this.version = version;
    this.catalogMap = new HashMap<>(8, 1.0F);
    this.catalogs = new ArrayList<>(8);
  }

  public void addCatalog(CatalogInfo catalogInfo, List<TableInfo> tableInfos) {
    if (catalogInfo.getState() != MetaState.Public) {
      return;
    }

    Catalog catalog = new Catalog(catalogInfo, tableInfos);
    this.catalogMap.put(catalog.getName().toLowerCase(), catalog);
    this.catalogs.add(catalog);
  }

  public Catalog getCatalog(String name) {
    Catalog catalog = catalogMap.get(name.toLowerCase());
    if (catalog == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_DB_ERROR, name);
    }
    return catalog;
  }

  public Catalog getCatalog(int id) {
    for (Catalog catalog : catalogs) {
      if (catalog.getId().intValue() == id) {
        return catalog;
      }
    }

    throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_DB_ERROR, String.valueOf(id));
  }

  public Table getTable(String catalog, String table) {
    Catalog db = this.getCatalog(catalog);
    return db.getTable(table);
  }

  public Table getTable(int catalog, int table) {
    Catalog db = this.getCatalog(catalog);
    return db.getTable(table);
  }

  public Collection<? extends Catalog> getAllCatalogs() {
    return new ArrayList<>(this.catalogs);
  }

  public long getVersion() {
    return this.version;
  }

  protected void retain() {
    REFCNT_UPDATER.incrementAndGet(this);
  }

  protected void release() {
    REFCNT_UPDATER.decrementAndGet(this);
  }

  protected boolean hasRef() {
    return REFCNT_UPDATER.get(this) > 0;
  }

  /**
   * MetaData Holder.
   */
  public static final class Holder {
    private static volatile MetaData metaData;

    private Holder() {
    }

    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    public static synchronized void set(MetaData meta) {
      MetaData oldMeta = metaData;
      metaData = meta;

      while (oldMeta != null && oldMeta.hasRef()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          LOG.error("MetaData set with await release Interrupted", ex);
        }
      }
    }

    public static MetaData get() {
      return metaData;
    }

    public static MetaData getRetained() {
      MetaData meta = metaData;
      meta.retain();
      return meta;
    }

    public static void release(MetaData meta) {
      meta.release();
    }
  }
}
