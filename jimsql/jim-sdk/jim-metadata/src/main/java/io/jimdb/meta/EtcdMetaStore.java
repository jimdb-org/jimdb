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
package io.jimdb.meta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.meta.client.EtcdClient;
import io.jimdb.pb.Ddlpb.IndexReorg;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.pb.Metapb;
import io.jimdb.core.plugin.MetaStore;
import io.etcd.jetcd.ByteSequence;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CHECKED", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS" })
public final class EtcdMetaStore implements MetaStore {
  // ROOT
  protected static final String SYSTEM_ROOT = "/sql";
  protected static final String INIT_PATH = SYSTEM_ROOT + "/init";
  protected static final String INIT_LOCK_PATH = SYSTEM_ROOT + "/lock";
  // Schema
  protected static final String SCHEMA_ROOT = SYSTEM_ROOT + "/schema";
  protected static final String SCHEMA_ID_PATH = SCHEMA_ROOT + "/id";
  protected static final String SCHEMA_VERSION_PATH = SCHEMA_ROOT + "/version";
  protected static final String SQL_NODE_ROOT = SCHEMA_ROOT + "/sqlnodes";
  protected static final String SQL_NODE_LIST = SQL_NODE_ROOT + "/";
  protected static final String SQL_NODE_PATH = SQL_NODE_ROOT + "/%s";
  protected static final String DB_ROOT = SCHEMA_ROOT + "/catalogs";
  protected static final String DB_LIST = DB_ROOT + "/";
  protected static final String DB_PATH = DB_ROOT + "/%s";
  protected static final String TABLE_ROOT = DB_PATH + "/tables";
  protected static final String TABLE_LIST = TABLE_ROOT + "/";
  protected static final String TABLE_PATH = TABLE_ROOT + "/%s";
  protected static final String TABLE_AUTO_ID_PATH = TABLE_PATH + "/autoid";
  // Task
  protected static final String TASK_ROOT = SCHEMA_ROOT + "/tasks";
  protected static final String TASK_LIST = TASK_ROOT + "/active/";
  protected static final String TASK_PATH = TASK_LIST + "%s";
  protected static final String TASK_ID_PATH = TASK_ROOT + "/id";
  protected static final String TASK_HISTORY_ROOT = TASK_ROOT + "/history";
  protected static final String TASK_HISTORY_LIST = TASK_HISTORY_ROOT + "/";
  protected static final String TASK_HISTORY_PATH = TASK_HISTORY_ROOT + "/%s";
  // Index Task
  protected static final String TASK_INDEX_ROOT = TASK_ROOT + "/index/";
  protected static final String TASK_INDEX_PATH = TASK_INDEX_ROOT + "%s";
  protected static final String TASK_INDEX_RANGE = TASK_INDEX_PATH + "/ranges/%s";
  protected static final String TASK_INDEX_RANGE_LIST = TASK_INDEX_PATH + "/ranges/";
  protected static final String TASK_INDEX_RANGE_LOCK = TASK_INDEX_PATH + "/locks/%s";

  // Lock
  protected static final String TASK_GLOBAL_LOCK_PATH = TASK_ROOT + "/lock/global";
  protected static final String TASK_TABLE_LOCK_PATH = TASK_ROOT + "/lock/table/%s";

  // Privilege
  protected static final String PRIVILEGE_ROOT = SYSTEM_ROOT + "/privilege";
  protected static final String PRI_VERSION_PATH = PRIVILEGE_ROOT + "/version";
  protected static final String TASK_PRI_ROOT = PRIVILEGE_ROOT + "/tasks";
  protected static final String TASK_PRI_LIST = TASK_PRI_ROOT + "/active/";
  protected static final String TASK_PRI_PATH = TASK_PRI_LIST + "%s";
  protected static final String TASK_PRI_ID_PATH = TASK_PRI_ROOT + "/id";
  protected static final String TASK_PRI_HISTORY_ROOT = TASK_PRI_ROOT + "/history";
  protected static final String TASK_PRI_HISTORY_LIST = TASK_PRI_HISTORY_ROOT + "/";
  protected static final String TASK_PRI_HISTORY_PATH = TASK_PRI_HISTORY_ROOT + "/%s";
  protected static final String TASK_PRI_LOCK_PATH = TASK_PRI_ROOT + "/lock/global";

  protected String serverID;
  protected EtcdClient etcdClient;

  @Override
  public void init(JimConfig c) {
    this.serverID = c.getServerID();
    this.etcdClient = EtcdClient.getInstance(c.getMetaStoreAddress(), 2 * c.getMetaLease(), TimeUnit.MILLISECONDS);
    this.initRootPath();
  }

  private void initRootPath() {
    int errCount = 0;
    while (true) {
      boolean isLock = false;
      try {
        isLock = etcdClient.tryLock(INIT_LOCK_PATH, serverID);
        if (!isLock) {
          continue;
        }

        ByteSequence value = etcdClient.get(INIT_PATH);
        if (value != null && "Initialized".equals(value.toString(StandardCharsets.UTF_8))) {
          return;
        }

        etcdClient.putIfAbsent(SCHEMA_ID_PATH, "100");
        etcdClient.putIfAbsent(SCHEMA_VERSION_PATH, "0");
        etcdClient.putIfAbsent(SQL_NODE_ROOT, "");
        etcdClient.putIfAbsent(DB_ROOT, "");
        etcdClient.putIfAbsent(TASK_ROOT, "");
        etcdClient.putIfAbsent(TASK_ID_PATH, "0");
        etcdClient.putIfAbsent(TASK_HISTORY_ROOT, "");

        etcdClient.putIfAbsent(PRI_VERSION_PATH, "1");
        etcdClient.putIfAbsent(TASK_PRI_ROOT, "");
        etcdClient.putIfAbsent(TASK_PRI_ID_PATH, "0");
        etcdClient.putIfAbsent(TASK_PRI_HISTORY_ROOT, "");
        etcdClient.put(INIT_PATH, ByteString.copyFrom("Initialized", StandardCharsets.UTF_8), false);
        return;
      } catch (JimException ex) {
        if (errCount++ > 10) {
          throw ex;
        }
      } finally {
        if (isLock) {
          etcdClient.unLock(INIT_LOCK_PATH, serverID);
        }
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Override
  public int allocMetaID() {
    return (int) etcdClient.incrAndGet(SCHEMA_ID_PATH, 1);
  }

  @Override
  public long allocTaskID(TaskType type) {
    long taskId = 0;
    switch (type) {
      case SCHEMATASK:
        taskId = etcdClient.incrAndGet(TASK_ID_PATH, 1);
        break;
      case PRITASK:
        taskId = etcdClient.incrAndGet(TASK_PRI_ID_PATH, 1);
        break;
      default:
        break;
    }
    return taskId;
  }

  @Override
  public long addAndGetVersion(long incr) {
    if (incr == 0) {
      ByteSequence value = etcdClient.get(SCHEMA_VERSION_PATH);
      return value == null ? 0 : Long.parseLong(value.toString(StandardCharsets.UTF_8));
    }
    return etcdClient.incrAndGet(SCHEMA_VERSION_PATH, incr);
  }

  @Override
  public long addAndGetPrivVersion(long incr) {
    if (incr == 0) {
      ByteSequence value = etcdClient.get(PRI_VERSION_PATH);
      return value == null ? 0 : Long.parseLong(value.toString(StandardCharsets.UTF_8));
    }
    return etcdClient.incrAndGet(PRI_VERSION_PATH, incr);
  }

  @Override
  public void register(long version) {
    etcdClient.put(String.format(SQL_NODE_PATH, serverID), ByteString.copyFrom(String.valueOf(version), StandardCharsets.UTF_8), true);
  }

  @Override
  public void unRegister() {
    etcdClient.delete(String.format(SQL_NODE_PATH, serverID));
  }

  @Override
  public Map<String, Long> getRegisters() {
    Map<String, Long> versions = new HashMap<>();
    Map<String, ByteSequence> kvs = etcdClient.listValues(SQL_NODE_LIST, false);
    if (kvs != null) {
      kvs.forEach((k, v) -> versions.put(k, Long.valueOf(v.toString(StandardCharsets.UTF_8))));
    }
    return versions;
  }

  @Override
  public boolean storeCatalog(Metapb.CatalogInfo expect, Metapb.CatalogInfo catalogInfo) {
    return etcdClient.comparePut(String.format(DB_PATH, String.valueOf(catalogInfo.getId())), expect == null ? null : expect.toByteString(), catalogInfo.toByteString());
  }

  @Override
  public boolean removeCatalog(int id) {
    etcdClient.delete(String.format(DB_PATH, String.valueOf(id)));
    return true;
  }

  @Override
  public Metapb.CatalogInfo getCatalog(int id) {
    ByteSequence value = etcdClient.get(String.format(DB_PATH, String.valueOf(id)));
    if (value == null) {
      return null;
    }

    try {
      return Metapb.CatalogInfo.parseFrom(value.getBytes());
    } catch (Throwable e) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Catalog Key(" + id + ")");
    }
  }

  @Override
  public List<Metapb.CatalogInfo> getCatalogs() {
    List<Metapb.CatalogInfo> catalogs = new ArrayList<>();
    Map<String, ByteSequence> kvs = etcdClient.listValues(DB_LIST, false);
    if (kvs != null) {
      kvs.forEach((k, v) -> {
        try {
          catalogs.add(Metapb.CatalogInfo.parseFrom(v.getBytes()));
        } catch (Throwable e) {
          throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Catalogs");
        }
      });
    }

    Collections.sort(catalogs, (c1, c2) -> {
      if (c1.getId() == c2.getId()) {
        return 0;
      }
      return c1.getId() > c2.getId() ? 1 : -1;
    });
    return catalogs;
  }

  @Override
  public boolean storeTable(Metapb.TableInfo expect, Metapb.TableInfo tableInfo) {
    return etcdClient.comparePut(String.format(TABLE_PATH, String.valueOf(tableInfo.getDbId()),
            String.valueOf(tableInfo.getId())), expect == null ? null : expect.toByteString(), tableInfo.toByteString());
  }

  @Override
  public boolean removeTable(int dbID, int tableID) {
    etcdClient.delete(String.format(TABLE_PATH, String.valueOf(dbID), String.valueOf(tableID)));
    return true;
  }

  @Override
  public Metapb.TableInfo getTable(int catalogID, int tableID) {
    ByteSequence value = etcdClient.get(String.format(TABLE_PATH, String.valueOf(catalogID), String.valueOf(tableID)));
    if (value == null) {
      return null;
    }

    try {
      return Metapb.TableInfo.parseFrom(value.getBytes());
    } catch (Throwable e) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Table Key(" + tableID + ")");
    }
  }

  @Override
  public List<Metapb.TableInfo> getTables(int catalogID) {
    List<Metapb.TableInfo> tableInfos = new ArrayList<>();
    Map<String, ByteSequence> kvs = etcdClient.listValues(String.format(TABLE_LIST, String.valueOf(catalogID)), false);
    if (kvs != null) {
      kvs.forEach((tk, tv) -> {
        try {
          tableInfos.add(Metapb.TableInfo.parseFrom(tv.getBytes()));
        } catch (Throwable e) {
          throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get tables");
        }
      });
    }

    Collections.sort(tableInfos, (c1, c2) -> {
      if (c1.getId() == c2.getId()) {
        return 0;
      }
      return c1.getId() > c2.getId() ? 1 : -1;
    });
    return tableInfos;
  }

  @Override
  public Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> getCatalogAndTables() {
    Map<Metapb.CatalogInfo, List<Metapb.TableInfo>> result = new HashMap<>();
    Map<String, ByteSequence> kvs = etcdClient.listValues(DB_LIST, true);

    if (kvs != null) {
      List<Metapb.CatalogInfo> catalogs = new ArrayList<>();
      kvs.forEach((k, v) -> {
        String tmp = StringUtils.removeStart(k, DB_LIST);
        if (tmp.indexOf('/') == -1) {
          try {
            catalogs.add(Metapb.CatalogInfo.parseFrom(v.getBytes()));
          } catch (Throwable e) {
            throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Catalogs");
          }
        }
      });
      Collections.sort(catalogs, (c1, c2) -> {
        if (c1.getId() == c2.getId()) {
          return 0;
        }
        return c1.getId() > c2.getId() ? 1 : -1;
      });

      for (Metapb.CatalogInfo catalog : catalogs) {
        List<Metapb.TableInfo> tables = new ArrayList<>();
        kvs.forEach((k, v) -> {
          String tmp = StringUtils.removeStart(k, String.format(TABLE_LIST, String.valueOf(catalog.getId())));
          if (tmp.indexOf('/') == -1) {
            try {
              tables.add(Metapb.TableInfo.parseFrom(v.getBytes()));
            } catch (Throwable e) {
              throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get tables");
            }
          }
        });

        Collections.sort(tables, (c1, c2) -> {
          if (c1.getId() == c2.getId()) {
            return 0;
          }
          return c1.getId() > c2.getId() ? 1 : -1;
        });
        result.put(catalog, tables);
      }
    }

    return result;
  }

  @Override
  public Metapb.AutoIdInfo getAutoIdInfo(int catalogID, int tableID) {
    ByteSequence value = etcdClient.get(String.format(TABLE_AUTO_ID_PATH, String.valueOf(catalogID), String.valueOf(tableID)));
    if (value == null) {
      return null;
    }

    try {
      return Metapb.AutoIdInfo.parseFrom(value.getBytes());
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Get AutoIdInfo Key(" + tableID + ")");
    }
  }

  @Override
  public Metapb.AutoIdInfo storeAutoIdInfo(int catalogID, int tableID, Metapb.AutoIdInfo autoIdInfo, Metapb.AutoIdInfo expectedAutoIdInfo) {
    ByteSequence key = ByteSequence.from(String.format(TABLE_AUTO_ID_PATH, String.valueOf(catalogID), String.valueOf(tableID)),
            StandardCharsets.UTF_8);
    ByteSequence updValue = ByteSequence.from(autoIdInfo.toByteString());

    ByteSequence existValue;
    if (expectedAutoIdInfo == null) {
      existValue = etcdClient.putIfAbsent(key, updValue, false);
    } else {
      ByteSequence expectedValue = ByteSequence.from(expectedAutoIdInfo.toByteString());
      existValue = etcdClient.comparePutAndGet(key, expectedValue, updValue, false);
    }
    if (existValue == null || existValue.equals(updValue)) {
      //put success
      return null;
    }
    //return old value
    try {
      return Metapb.AutoIdInfo.parseFrom(existValue.getBytes());
    } catch (Throwable e) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "store AutoIdInfo Key(" + tableID + ")");
    }
  }

  @Override
  public boolean storeTask(TaskType type, Task expect, Task... tasks) {
    if (type == TaskType.INDEXTASK) {
      return storeIndexTask(expect, tasks);
    }

    final Task task = tasks[0];
    final String taskId = String.valueOf(task.getId());
    switch (type) {
      case SCHEMATASK:
        return etcdClient.comparePut(String.format(TASK_PATH, taskId), expect == null ? null : expect.toByteString(), task.toByteString());
      case PRITASK:
        return etcdClient.comparePut(String.format(TASK_PRI_PATH, taskId), expect == null ? null : expect.toByteString(), task.toByteString());
      case SCHEMAHISTORY:
        return etcdClient.comparePut(String.format(TASK_HISTORY_PATH, taskId), expect == null ? null : expect.toByteString(), task.toByteString());
      case PRIHISTORY:
        return etcdClient.comparePut(String.format(TASK_PRI_HISTORY_PATH, taskId), expect == null ? null : expect.toByteString(), task.toByteString());
      default:
        return false;
    }
  }

  private boolean storeIndexTask(Task expect, Task... tasks) {
    if (expect == null) {
      String id = String.valueOf(tasks[0].getId());
      List<Tuple2<String, ByteString>> values = new ArrayList<>(tasks.length + 1);
      Task indexStatus = Task.newBuilder()
              .setId(tasks[0].getId())
              .setState(TaskState.Running)
              .build();
      values.add(Tuples.of(String.format(TASK_INDEX_PATH, id), indexStatus.toByteString()));
      try {
        for (Task task : tasks) {
          IndexReorg reorg = IndexReorg.parseFrom(task.getData());
          values.add(Tuples.of(String.format(TASK_INDEX_RANGE, id, String.valueOf(reorg.getOffset())), task.toByteString()));
        }
      } catch (InvalidProtocolBufferException e) {
        throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Store IndexTask");
      }

      return etcdClient.putIfAbsent(values);
    }

    Task task = tasks[0];
    try {
      IndexReorg reorg = IndexReorg.parseFrom(task.getData());
      return etcdClient.comparePut(String.format(TASK_INDEX_RANGE, String.valueOf(task.getId()), String.valueOf(reorg.getOffset())),
              expect.toByteString(), task.toByteString());
    } catch (InvalidProtocolBufferException e) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Store IndexTask");
    }
  }

  @Override
  public Task getTask(TaskType type, Task task) {
    final ByteSequence value;
    final String id = String.valueOf(task.getId());

    switch (type) {
      case SCHEMATASK:
        value = etcdClient.get(String.format(TASK_PATH, id));
        break;
      case PRITASK:
        value = etcdClient.get(String.format(TASK_PRI_PATH, id));
        break;
      case SCHEMAHISTORY:
        value = etcdClient.get(String.format(TASK_HISTORY_PATH, id));
        break;
      case PRIHISTORY:
        value = etcdClient.get(String.format(TASK_PRI_HISTORY_PATH, id));
        break;
      case INDEXTASK:
        final String key;
        if (task.getData() == null) {
          key = String.format(TASK_INDEX_PATH, id);
        } else {
          try {
            IndexReorg reorg = IndexReorg.parseFrom(task.getData());
            key = String.format(TASK_INDEX_RANGE, id, String.valueOf(reorg.getOffset()));
          } catch (InvalidProtocolBufferException e) {
            throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Task");
          }
        }

        value = etcdClient.get(key);
        break;
      default:
        value = null;
        break;
    }

    try {
      if (value == null) {
        return null;
      }

      return Task.parseFrom(value.getBytes());
    } catch (InvalidProtocolBufferException e) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Task");
    }
  }

  @Override
  public List<Task> getTasks(TaskType type, Task task) {
    final Map<String, ByteSequence> values;
    final List<Task> tasks = new ArrayList<>();

    switch (type) {
      case SCHEMATASK:
        values = etcdClient.listValues(TASK_LIST, false);
        break;
      case PRITASK:
        values = etcdClient.listValues(TASK_PRI_LIST, false);
        break;
      case SCHEMAHISTORY:
        values = etcdClient.listValues(TASK_HISTORY_LIST, false);
        break;
      case PRIHISTORY:
        values = etcdClient.listValues(TASK_PRI_HISTORY_LIST, false);
        break;
      case INDEXTASK:
        final String key;
        if (task == null) {
          key = TASK_INDEX_ROOT;
        } else {
          key = String.format(TASK_INDEX_RANGE_LIST, String.valueOf(task.getId()));
        }
        values = etcdClient.listValues(key, true);
        break;
      default:
        values = null;
        break;
    }

    if (values != null) {
      values.forEach((k, v) -> {
        try {
          if (type == TaskType.INDEXTASK && k.indexOf("/ranges/") < 0) {
            return;
          }
          tasks.add(Task.parseFrom(v.getBytes()));
        } catch (Throwable e1) {
          throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, "Get Tasks");
        }
      });
    }

    Collections.sort(tasks, (c1, c2) -> {
      if (c1.getId() == c2.getId()) {
        if (type == TaskType.INDEXTASK) {
          try {
            IndexReorg reorg1 = IndexReorg.parseFrom(c1.getData());
            IndexReorg reorg2 = IndexReorg.parseFrom(c2.getData());
            return reorg1.getOffset() > reorg2.getOffset() ? 1 : -1;
          } catch (InvalidProtocolBufferException e) {
            throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Get Task");
          }
        }

        return 0;
      }
      return c1.getId() > c2.getId() ? 1 : -1;
    });
    return tasks;
  }

  @Override
  public void removeTask(TaskType type, long taskId) {
    final String id = String.valueOf(taskId);
    switch (type) {
      case SCHEMATASK:
        this.etcdClient.delete(String.format(TASK_PATH, id));
        break;
      case PRITASK:
        this.etcdClient.delete(String.format(TASK_PRI_PATH, id));
        break;
      case SCHEMAHISTORY:
        this.etcdClient.delete(String.format(TASK_HISTORY_PATH, id));
        break;
      case PRIHISTORY:
        this.etcdClient.delete(String.format(TASK_PRI_HISTORY_PATH, id));
        break;
      case INDEXTASK:
        this.etcdClient.delete(String.format(TASK_INDEX_PATH, id));
        break;
      default:
        break;
    }
  }

  @Override
  public boolean tryLock(TaskType type, Task task) {
    switch (type) {
      case SCHEMATASK:
        if (MetaStore.isGlobalTask(task.getOp())) {
          return etcdClient.tryLock(TASK_GLOBAL_LOCK_PATH, serverID);
        }
        return etcdClient.tryLock(String.format(TASK_TABLE_LOCK_PATH, String.valueOf(task.getTableId())), serverID);
      case PRITASK:
        return etcdClient.tryLock(TASK_PRI_LOCK_PATH, serverID);
      case INDEXTASK:
        try {
          IndexReorg reorg = IndexReorg.parseFrom(task.getData());
          return etcdClient.tryLock(String.format(TASK_INDEX_RANGE_LOCK, String.valueOf(task.getId()), String.valueOf(reorg.getOffset())), serverID);
        } catch (InvalidProtocolBufferException e) {
          throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Lock Task");
        }
      default:
        return false;
    }
  }

  @Override
  public void unLock(TaskType type, Task task) {
    switch (type) {
      case SCHEMATASK:
        if (MetaStore.isGlobalTask(task.getOp())) {
          etcdClient.unLock(TASK_GLOBAL_LOCK_PATH, serverID);
          return;
        }
        etcdClient.unLock(String.format(TASK_TABLE_LOCK_PATH, String.valueOf(task.getTableId())), serverID);
        break;
      case PRITASK:
        etcdClient.unLock(TASK_PRI_LOCK_PATH, serverID);
        break;
      case INDEXTASK:
        try {
          IndexReorg reorg = IndexReorg.parseFrom(task.getData());
          etcdClient.unLock(String.format(TASK_INDEX_RANGE_LOCK, String.valueOf(task.getId()), String.valueOf(reorg.getOffset())), serverID);
        } catch (InvalidProtocolBufferException e) {
          throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, e, "Lock Task");
        }
        break;
      default:
        break;
    }
  }

  @Override
  public void watch(WatchType type, LongConsumer consumer) {
    switch (type) {
      case SCHEMAVERSION:
        etcdClient.watch(SCHEMA_VERSION_PATH, false, consumer);
        break;
      case SCHEMATASK:
        etcdClient.watch(TASK_LIST, true, consumer);
        break;
      case PRIVERSION:
        etcdClient.watch(PRI_VERSION_PATH, false, consumer);
        break;
      case PRITASK:
        etcdClient.watch(TASK_PRI_LIST, true, consumer);
        break;
      case INDEXTASK:
        etcdClient.watch(TASK_INDEX_ROOT, true, consumer);
        break;
      default:
        break;
    }
  }

  @Override
  public void close() {
    if (etcdClient != null) {
      etcdClient.close();
    }
  }
}
