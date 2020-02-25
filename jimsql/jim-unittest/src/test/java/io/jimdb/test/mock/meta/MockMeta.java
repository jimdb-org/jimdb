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
package io.jimdb.test.mock.meta;

import java.util.List;

import io.jimdb.core.model.meta.MetaData;
import io.jimdb.pb.Metapb;

import com.google.common.collect.Lists;

/**
 * @version V1.0
 */
public abstract class MockMeta {
  public void init() {
    MetaData metaData = new MetaData(0);
    Metapb.CatalogInfo catalogInfo =
            Metapb.CatalogInfo.newBuilder().setId(1).setName("test").setState(Metapb.MetaState.Public).build();
    List<Metapb.TableInfo> tableInfos = Lists.newArrayListWithExpectedSize(10);
    tableInfos.addAll(initTables());
    metaData.addCatalog(catalogInfo, tableInfos);
    MetaData.Holder.set(metaData);
  }

  protected abstract List<Metapb.TableInfo> initTables();
}
