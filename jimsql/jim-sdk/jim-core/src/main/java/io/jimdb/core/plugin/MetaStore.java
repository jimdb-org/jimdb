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
package io.jimdb.core.plugin;

import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Metapb.AutoIdInfo;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.TableInfo;

/**
 * The interface is responsible for fetch and store data from metastore.
 * All metadata of the system: databases, tables, index.
 *
 * @version V1.0
 * @ThreadSafe
 */
public interface MetaStore extends Plugin {
  int allocMetaID();

  long allocTaskID(TaskType type);

  long addAndGetVersion(long incr);

  long addAndGetPrivVersion(long incr);

  void register(long version);

  void unRegister();

  Map<String, Long> getRegisters();

  boolean storeCatalog(CatalogInfo expect, CatalogInfo catalogInfo);

  boolean removeCatalog(int id);

  CatalogInfo getCatalog(int id);

  List<CatalogInfo> getCatalogs();

  boolean storeTable(TableInfo expect, TableInfo tableInfo);

  boolean removeTable(int catalogID, int tableID);

  TableInfo getTable(int catalogID, int tableID);

  List<TableInfo> getTables(int catalogID);

  Map<CatalogInfo, List<TableInfo>> getCatalogAndTables();

  AutoIdInfo getAutoIdInfo(int catalogID, int tableID);

  AutoIdInfo storeAutoIdInfo(int catalogID, int tableID, AutoIdInfo autoIdInfo, AutoIdInfo expect);

  boolean storeTask(TaskType type, Task expect, Task... task);

  Task getTask(TaskType type, Task task);

  List<Task> getTasks(TaskType type, Task task);

  void removeTask(TaskType type, long taskID);

  boolean tryLock(TaskType type, Task task);

  void unLock(TaskType type, Task task);

  void watch(WatchType type, LongConsumer consumer);

  static boolean isGlobalTask(OpType type) {
    switch (type) {
      case CreateCatalog:
      case DropCatalog:
      case CreateTable:
        return true;

      default:
        return false;
    }
  }

  /**
   * Task type enum.
   */
  enum TaskType {
    SCHEMATASK, INDEXTASK, SCHEMAHISTORY, PRITASK, PRIHISTORY
  }

  /**
   * Watch type enum.
   */
  enum WatchType {
    SCHEMAVERSION, SCHEMATASK, INDEXTASK, PRIVERSION, PRITASK
  }
}
