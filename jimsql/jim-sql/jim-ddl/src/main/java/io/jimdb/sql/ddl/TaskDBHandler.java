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
package io.jimdb.sql.ddl;

import java.util.List;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Mspb.DeleteRangesRequest;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.RouterStore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Responsible for handling database task.
 *
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
final class TaskDBHandler {
  private TaskDBHandler() {
  }

  static CatalogInfo createCatalog(MetaStore metaStore, CatalogInfo catalog) {
    final List<CatalogInfo> existCatalogs = metaStore.getCatalogs();
    if (existCatalogs != null) {
      for (CatalogInfo existCatalog : existCatalogs) {
        if (existCatalog.getId() == catalog.getId()) {
          if (existCatalog.getState() == MetaState.Public) {
            return catalog.toBuilder().setState(MetaState.Public).build();
          }
        } else if (existCatalog.getName().equalsIgnoreCase(catalog.getName())) {
          throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DB_CREATE_EXISTS, catalog.getName());
        }
      }
    }

    switch (catalog.getState()) {
      case Absent:
        catalog = catalog.toBuilder()
                .setState(MetaState.Public)
                .build();
        if (!metaStore.storeCatalog(null, catalog)) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Catalog[%d] occur concurrent process error", catalog.getId()));
        }
        return catalog.toBuilder().setState(MetaState.Public).build();

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, String.format("invalid catalog state %s", catalog.getState().name()));
    }
  }

  static MetaState dropCatalog(MetaStore metaStore, RouterStore routerStore, int dbId) {
    CatalogInfo.Builder builder = null;
    CatalogInfo oldCatalog = null;
    MetaState state = MetaState.Absent;

    oldCatalog = metaStore.getCatalog(dbId);
    if (oldCatalog != null) {
      state = oldCatalog.getState();
      builder = oldCatalog.toBuilder();
    }

    switch (state) {
      case Public:
        builder.setState(MetaState.WriteOnly);
        if (!metaStore.storeCatalog(oldCatalog, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Catalog[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.WriteOnly;
      case WriteOnly:
        builder.setState(MetaState.DeleteOnly);
        if (!metaStore.storeCatalog(oldCatalog, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Catalog[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.DeleteOnly;
      case DeleteOnly:
        builder.setState(MetaState.Absent);
        if (!metaStore.storeCatalog(oldCatalog, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Catalog[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.DeleteOnly;
      default:
        doDropCatalog(metaStore, routerStore, dbId);
        return MetaState.Absent;
    }
  }

  private static void doDropCatalog(MetaStore metaStore, RouterStore routerStore, int id) {
    DeleteRangesRequest.Builder builder = DeleteRangesRequest.newBuilder();
    builder.setDbId(id);
    routerStore.deleteRange(builder.build());
    metaStore.removeCatalog(id);
  }
}
