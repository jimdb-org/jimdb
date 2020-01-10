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
package io.jimdb.meta;

import static io.jimdb.meta.EtcdMetaStore.SQL_NODE_LIST;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.config.JimConfig;
import io.jimdb.meta.client.EtcdClient;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.common.utils.lang.IOUtil;
import io.etcd.jetcd.ByteSequence;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class EtcdMetaStoreTest {
  private static EtcdMetaStore metaStore;
  private static EtcdClient client;
  private static JimConfig config;

  @BeforeClass
  public static void beforeClass() {
    Properties properties = IOUtil.loadResource("jim.properties");

    config = new JimConfig(properties);

    client = EtcdClient.getInstance(config.getMetaStoreAddress(), 2 * config.getMetaLease(), TimeUnit.MILLISECONDS);
    client.delete(EtcdMetaStore.SCHEMA_ROOT);

    metaStore = new EtcdMetaStore();
    metaStore.init(config);
  }

  @AfterClass
  public static void afterClass() {
    client.delete(EtcdMetaStore.SCHEMA_ROOT);
    metaStore.close();
  }

  @Before
  public void setUp() {
    client.delete(EtcdMetaStore.DB_LIST);
  }

  @Test
  public void testAllocMetaID() throws Exception {
    int parallel = 16;
    ConcurrentHashMap<Integer, Boolean> ids = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(parallel);

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          for (int j = 0; j < 100; j++) {
            Integer id = metaStore.allocMetaID();
            if (ids.putIfAbsent(id, Boolean.TRUE) != null) {
              Assert.fail("AllocMetaID Duplicate");
            }
          }
          latch.countDown();
        }
      };
      t.start();
    }

    latch.await();
    ByteSequence value = client.get(EtcdMetaStore.SCHEMA_ID_PATH);
    Assert.assertNotNull(value);
    Assert.assertEquals("1600", value.toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testAllocTaskID() throws Exception {
    int parallel = 16;
    ConcurrentHashMap<Long, Boolean> ids = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(parallel);

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          for (int j = 0; j < 100; j++) {
            Long id = metaStore.allocTaskID(MetaStore.TaskType.SCHEMATASK);
            if (ids.putIfAbsent(id, Boolean.TRUE) != null) {
              Assert.fail("AllocTaskID Duplicate");
            }
          }
          latch.countDown();
        }
      };
      t.start();
    }

    latch.await();
    ByteSequence value = client.get(EtcdMetaStore.TASK_ID_PATH);
    Assert.assertNotNull(value);
    Assert.assertEquals("1600", value.toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testAddAndGetVersion() throws Exception {
    int parallel = 16;
    ConcurrentHashMap<Long, Boolean> ids = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(parallel);

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          for (int j = 0; j < 100; j++) {
            Long id = metaStore.addAndGetVersion(1);
            if (ids.putIfAbsent(id, Boolean.TRUE) != null) {
              Assert.fail("AllocVersion Duplicate");
            }
          }
          latch.countDown();
        }
      };
      t.start();
    }

    latch.await();
    long id = metaStore.addAndGetVersion(0);
    Assert.assertEquals(1600, id);
  }

  @Test
  public void testGetCatalogs() throws Exception {
    int parallel = 16;
    CountDownLatch latch = new CountDownLatch(parallel);
    List<Integer> ids = new ArrayList<>();

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          CatalogInfo.Builder builder = CatalogInfo.newBuilder();
          int id = metaStore.allocMetaID();
          ids.add(id);
          builder.setId(id)
                  .setName("test_" + id);
          metaStore.storeCatalog(null, builder.build());

          for (int j = 0; j < 5; j++) {
            TableInfo.Builder builder1 = TableInfo.newBuilder();
            int id1 = metaStore.allocMetaID();
            builder1.setDbId(builder.getId())
                    .setId(id1)
                    .setName("test_table_" + id1);
            metaStore.storeTable(null, builder1.build());
          }
          latch.countDown();
        }
      };
      t.start();
    }

    latch.await();
    List<CatalogInfo> catalogs = metaStore.getCatalogs();
    Assert.assertEquals(parallel, catalogs.size());
    Collections.sort(ids, (c1, c2) -> {
      if (c1.intValue() == c2.intValue()) {
        return 0;
      }
      return c1.intValue() > c2.intValue() ? 1 : -1;
    });

    for (int i = 0; i < catalogs.size(); i++) {
      CatalogInfo catalogInfo = catalogs.get(i);
      int id = ids.get(i).intValue();
      Assert.assertEquals(id, catalogInfo.getId());
      Assert.assertEquals("test_" + id, catalogInfo.getName());
    }
  }

  @Test
  public void testGetTables() throws Exception {
    int parallel = 16;
    CountDownLatch latch = new CountDownLatch(parallel);
    HashMap<Integer, List<TableInfo>> listHashMap = new HashMap();

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread(() -> {
        List<TableInfo> tableInfos = new ArrayList<>();
        CatalogInfo.Builder builder = CatalogInfo.newBuilder();
        int id = metaStore.allocMetaID();
        builder.setId(id)
                .setName("test_" + id);
        metaStore.storeCatalog(null, builder.build());

        for (int j = 0; j < 5; j++) {
          TableInfo.Builder builder1 = TableInfo.newBuilder();
          int id1 = metaStore.allocMetaID();
          TableInfo tableInfo = builder1.setDbId(builder.getId())
                  .setId(id1)
                  .setName("test_table_" + id1).build();
          metaStore.storeTable(null, tableInfo);
          tableInfos.add(tableInfo);
        }
        listHashMap.put(id, tableInfos);
        latch.countDown();
      });
      t.start();
    }

    latch.await();
    List<CatalogInfo> catalogs = metaStore.getCatalogs();
    Assert.assertEquals(parallel, catalogs.size());

    catalogs.forEach(e -> {
      List<TableInfo> ts = listHashMap.get(e.getId());
      Collections.sort(ts, (c1, c2) -> {
        if (c1.getId() == c2.getId()) {
          return 0;
        }
        return c1.getId() > c2.getId() ? 1 : -1;
      });
      List<TableInfo> tables = metaStore.getTables(e.getId());
      for (int i = 0; i < tables.size(); i++) {
        int metaTableId = tables.get(i).getId();
        int currentId = ts.get(i).getId();
        String metaTableName = tables.get(i).getName();
        String currentName = ts.get(i).getName();
        Assert.assertEquals(currentId, metaTableId);
        Assert.assertEquals(currentName, metaTableName);
      }
    });
  }

  @Test
  public void registerTest() {

    HashMap<String, Long> etcdMetaStores = new HashMap<>();
    List<EtcdMetaStore> metaStores = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      EtcdMetaStore metaStore = new EtcdMetaStore();
      Properties properties = IOUtil.loadResource("jim.properties");
      JimConfig config = new JimConfig(properties);

      metaStore.init(config);

      metaStore.register(i);
      etcdMetaStores.put(SQL_NODE_LIST + metaStore.serverID, (long) i);
      metaStores.add(metaStore);
    }

    Map<String, Long> registers = metaStore.getRegisters();
    registers.forEach((k, v) -> {
      Long version = etcdMetaStores.get(k);
      Assert.assertEquals(version, v);
    });
    metaStores.forEach(e -> e.unRegister());

    Map<String, Long> noRegisters = metaStore.getRegisters();
    Assert.assertEquals(0, noRegisters.size());
  }

  @Test
  public void storeCatalog() {
    CatalogInfo.Builder builder = CatalogInfo.newBuilder();
    int id = metaStore.allocMetaID();
    builder.setId(id)
            .setName("test_" + id);
    metaStore.storeCatalog(null, builder.build());
    CatalogInfo catalog1 = metaStore.getCatalog(id);
    Assert.assertEquals("test_" + id, catalog1.getName());

    metaStore.removeCatalog(id);

    CatalogInfo catalog3 = metaStore.getCatalog(id);
    Assert.assertEquals(null, catalog3);
  }

  @Test
  public void storeTable() {
    int dbId = metaStore.allocMetaID();

    CatalogInfo.Builder dbBuilder = CatalogInfo.newBuilder();
    dbBuilder.setId(dbId)
            .setName("test_" + dbId);

    metaStore.storeCatalog(null, dbBuilder.build());
    CatalogInfo catalog = metaStore.getCatalog(dbId);
    Assert.assertEquals(dbId, catalog.getId());
    Assert.assertEquals("test_" + dbId, catalog.getName());

    TableInfo.Builder builder = TableInfo.newBuilder();
    int tableId = metaStore.allocMetaID();
    builder.setId(tableId)
            .setName("test_table" + tableId).setDbId(dbId);
    metaStore.storeTable(null, builder.build());

    TableInfo table1 = metaStore.getTable(dbId, tableId);
    Assert.assertEquals("test_table" + tableId, table1.getName());
  }

  @Test
  public void lockTest() throws InterruptedException {
    int dbId = metaStore.allocMetaID();

    CatalogInfo.Builder dbBuilder = CatalogInfo.newBuilder();
    dbBuilder.setId(dbId)
            .setName("test_" + dbId);

    metaStore.storeCatalog(null, dbBuilder.build());
    CatalogInfo catalog = metaStore.getCatalog(dbId);
    Assert.assertEquals(dbId, catalog.getId());
    Assert.assertEquals("test_" + dbId, catalog.getName());

    TableInfo.Builder builder = TableInfo.newBuilder();
    int tableId = metaStore.allocMetaID();
    builder.setId(tableId)
            .setName("test_table" + tableId).setDbId(dbId);

    int parallel = 16;
    CountDownLatch latch = new CountDownLatch(parallel);
    ConcurrentHashMap<Integer, EtcdMetaStore> storeMap = new ConcurrentHashMap();
    for (int i = 0; i < parallel; i++) {
      EtcdMetaStore metaStore = new EtcdMetaStore();
      Properties properties = IOUtil.loadResource("jim.properties");
      JimConfig config = new JimConfig(properties);
      metaStore.init(config);
      storeMap.put(i, metaStore);
    }

    ConcurrentHashMap<String, EtcdMetaStore> lockedMap = new ConcurrentHashMap();
    ConcurrentHashMap<String, EtcdMetaStore> nolockMap = new ConcurrentHashMap();

    for (int i = 0; i < parallel; i++) {
      EtcdMetaStore etcdMetaStore = storeMap.get(i);
      Thread t = new Thread(() -> {
        boolean locked = etcdMetaStore.tryLock(MetaStore.TaskType.SCHEMATASK, Ddlpb.Task.newBuilder()
                .setOp(Ddlpb.OpType.AlterTable).setTableId(tableId).build());
        if (locked) {
          lockedMap.put(etcdMetaStore.serverID, etcdMetaStore);
        } else {
          nolockMap.put(etcdMetaStore.serverID, etcdMetaStore);
        }
        latch.countDown();
      });
      t.start();
    }
    latch.await();

    Assert.assertEquals(lockedMap.size(), 1);
    Assert.assertEquals(nolockMap.size(), 15);

    lockedMap.forEach((k, v) -> {
      boolean lock = v.tryLock(MetaStore.TaskType.SCHEMATASK, Ddlpb.Task.newBuilder()
              .setOp(Ddlpb.OpType.AlterTable).setTableId(tableId).build());
      Assert.assertEquals(lock, true);
    });

    nolockMap.forEach((k, v) -> {
      boolean lock = v.tryLock(MetaStore.TaskType.SCHEMATASK, Ddlpb.Task.newBuilder()
              .setOp(Ddlpb.OpType.AlterTable).setTableId(tableId).build());
      Assert.assertEquals(lock, false);
    });

    //unlock test
    lockedMap.forEach((k, v) -> v.unLock(MetaStore.TaskType.SCHEMATASK, Ddlpb.Task.newBuilder()
            .setOp(Ddlpb.OpType.AlterTable).setTableId(tableId).build()));
    EtcdMetaStore metaStore = new EtcdMetaStore();
    Properties properties = IOUtil.loadResource("jim.properties");
    JimConfig config = new JimConfig(properties);
    metaStore.init(config);
    boolean lock = metaStore.tryLock(MetaStore.TaskType.SCHEMATASK, Ddlpb.Task.newBuilder()
            .setOp(Ddlpb.OpType.AlterTable).setTableId(tableId).build());
    Assert.assertEquals(lock, true);

    metaStore.removeCatalog(dbId);
    TableInfo table3 = metaStore.getTable(dbId, tableId);
    Assert.assertEquals(null, table3);
  }

  @Test
  public void taskTest() throws InterruptedException {
    long taskID = metaStore.allocTaskID(MetaStore.TaskType.SCHEMATASK);
    Ddlpb.Task task = Ddlpb.Task.newBuilder().setId(taskID).setMetaState(Metapb.MetaState.Absent).build();
    metaStore.storeTask(MetaStore.TaskType.SCHEMATASK, null, task);
    Ddlpb.Task metaStoreTask = metaStore.getTask(MetaStore.TaskType.SCHEMATASK, task);

    Assert.assertEquals(taskID, metaStoreTask.getId());
    Assert.assertEquals(Metapb.MetaState.Absent, metaStoreTask.getMetaState());

    metaStore.removeTask(MetaStore.TaskType.SCHEMATASK, taskID);
    Ddlpb.Task task1 = metaStore.getTask(MetaStore.TaskType.SCHEMATASK, task);

    Assert.assertEquals(task1, null);

    int parallel = 16;
    CountDownLatch latch = new CountDownLatch(parallel);
    ConcurrentHashMap<Long, Ddlpb.Task> taskMap = new ConcurrentHashMap();

    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread(() -> {
        long id = metaStore.allocTaskID(MetaStore.TaskType.SCHEMATASK);
        Ddlpb.Task task2 = Ddlpb.Task.newBuilder()
                .setId(id)
                .setMetaState(Metapb.MetaState.Absent)
                .build();
        metaStore.storeTask(MetaStore.TaskType.SCHEMATASK, null, task2);
        taskMap.put(id, task2);
        latch.countDown();
      });
      t.start();
    }
    latch.await();

    List<Ddlpb.Task> tasks1 = metaStore.getTasks(MetaStore.TaskType.SCHEMATASK, null);
    Assert.assertEquals(tasks1.size(), 16);
    tasks1.forEach(e -> {
      Ddlpb.Task task2 = taskMap.get(e.getId());
      Assert.assertEquals(e.getMetaState(), task2.getMetaState());
      metaStore.removeTask(MetaStore.TaskType.SCHEMATASK, e.getId());
    });
    List<Ddlpb.Task> tasks2 = metaStore.getTasks(MetaStore.TaskType.SCHEMATASK, null);
    Assert.assertEquals(tasks2.size(), 0);
  }

  @Test
  public void watchTest() throws InterruptedException {
    int parallel = 16;
    CountDownLatch latch = new CountDownLatch(parallel);
    ConcurrentHashMap<Integer, EtcdMetaStore> storeMap = new ConcurrentHashMap();

    for (int i = 0; i < parallel; i++) {
      EtcdMetaStore metaStore = new EtcdMetaStore();
      Properties properties = IOUtil.loadResource("jim.properties");
      JimConfig config = new JimConfig(properties);
      metaStore.init(config);
      storeMap.put(i, metaStore);
    }

    storeMap.forEach((k, v) -> {
      Thread t = new Thread(() -> {
        v.watch(MetaStore.WatchType.SCHEMAVERSION, value -> Assert.assertEquals(value, 0));
        latch.countDown();
      });
      t.start();
    });
    latch.await();

    for (int i = 0; i < parallel; i++) {
      metaStore.addAndGetVersion(1);
    }
    TimeUnit.SECONDS.sleep(10);
  }
}
