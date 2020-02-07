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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.pb.Ddlpb.AddIndexInfo;
import io.jimdb.pb.Ddlpb.AlterTableInfo;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Responsible for performing ddl operations.
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "OCP_OVERLY_CONCRETE_PARAMETER" })
public final class DDLExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DDLExecutor.class);

  private static long delay;
  private static MetaStore metaStore;
  private static RouterStore routerStore;
  private static Engine storeEngine;
  private static DDLSyncer syncer;
  private static DDLWorker worker;
  private static IndexWorker indexWorker;
  private static ScheduledExecutorService responseExecutor;

  private DDLExecutor() {
  }

  public static void init(JimConfig config) {
    metaStore = PluginFactory.getMetaStore();
    routerStore = PluginFactory.getRouterStore();
    storeEngine = PluginFactory.getStoreEngine();

    final long lease = config.getMetaLease();
    delay = Math.min(2 * lease, 1000);
    syncer = new DDLSyncer(metaStore, 2 * lease);
    syncer.start();
    worker = new DDLWorker(metaStore, routerStore, storeEngine, syncer);
    worker.start();
    indexWorker = new IndexWorker(metaStore, storeEngine, config.getReorgThreads());
    indexWorker.start();
    responseExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("DDL-TaskResp-Executor", true));
  }

  public static void close() {
    if (worker != null) {
      worker.close();
    }
    if (indexWorker != null) {
      indexWorker.close();
    }
    if (syncer != null) {
      syncer.close();
    }
    if (responseExecutor != null) {
      responseExecutor.shutdown();
    }
  }

  public static Flux<Boolean> createCatalog(CatalogInfo catalogInfo) {
    int catalogID = metaStore.allocMetaID();
    CatalogInfo.Builder catalogBuilder = catalogInfo.toBuilder();
    catalogBuilder.setId(catalogID);
    return executeCommand(catalogID, 0, OpType.CreateCatalog, catalogBuilder.build());
  }

  public static Flux<Boolean> dropCatalog(CatalogInfo catalogInfo) {
    return executeCommand(0, 0, OpType.DropCatalog, catalogInfo);
  }

  public static Flux<Boolean> createTable(TableInfo tableInfo) {
    int tableID = metaStore.allocMetaID();
    TableInfo.Builder tableBuilder = tableInfo.toBuilder();
    tableBuilder.setId(tableID)
            .setState(MetaState.Absent);

    for (int i = 0; i < tableInfo.getColumnsList().size(); i++) {
      ColumnInfo column = tableInfo.getColumnsList().get(i);
      ColumnInfo.Builder builder = column.toBuilder();
      builder.setState(MetaState.Public);
      tableBuilder.setColumns(i, builder.build());
    }
    for (int i = 0; i < tableInfo.getIndicesList().size(); i++) {
      IndexInfo index = tableInfo.getIndicesList().get(i);
      IndexInfo.Builder builder = index.toBuilder();
      builder.setId(metaStore.allocMetaID())
              .setTableId(tableID)
              .setState(MetaState.Public);
      tableBuilder.setIndices(i, builder.build());
    }

    return executeCommand(0, tableID, OpType.CreateTable, tableBuilder.build());
  }

  public static Flux<Boolean> addIndex(AlterTableInfo alterInfo) {
    try {
      AddIndexInfo index = AddIndexInfo.parseFrom(alterInfo.getItem());
      index = index.toBuilder()
              .setId(metaStore.allocMetaID())
              .setState(MetaState.Absent)
              .build();

      alterInfo = alterInfo.toBuilder()
              .setItem(index.toByteString())
              .setState(MetaState.Absent)
              .build();
      return alterTable(alterInfo);
    } catch (InvalidProtocolBufferException ex) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, ex);
    }
  }

  public static Flux<Boolean> alterTable(AlterTableInfo alterInfo) {
    return executeCommand(0, 0, OpType.AlterTable, alterInfo);
  }

  private static Flux<Boolean> executeCommand(int dbID, int tableID, OpType op, Message metaInfo) {
    final Task.Builder taskBuilder = Task.newBuilder();
    taskBuilder.setId(metaStore.allocTaskID(TaskType.SCHEMATASK))
            .setDbId(dbID)
            .setTableId(tableID)
            .setOp(op)
            .setData(metaInfo.toByteString())
            .setMetaVersion(MetaData.Holder.get().getVersion())
            .setCreateTime(SystemClock.currentTimeMillis())
            .setState(TaskState.Init)
            .setMetaState(MetaState.Absent);

    final Task task = taskBuilder.build();
    metaStore.storeTask(TaskType.SCHEMATASK, null, task);
    return responseTask(task);
  }

  private static Flux<Boolean> responseTask(final Task task) {
    return Flux.create(sink -> {
      TaskResponseSyncer syncer = new TaskResponseSyncer(task.getId(), sink);
      syncer.start();
    });
  }

  /**
   * Task response syncer.
   */
  static class TaskResponseSyncer implements Runnable {
    private final long taskID;
    private final FluxSink<Boolean> sink;
    private ScheduledFuture<?> future;

    TaskResponseSyncer(long taskID, FluxSink<Boolean> sink) {
      this.sink = sink;
      this.taskID = taskID;
    }

    void start() {
      future = responseExecutor.scheduleWithFixedDelay(this, delay, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      Task task = null;
      try {
        task = metaStore.getTask(TaskType.SCHEMAHISTORY, Task.newBuilder().setId(taskID).build());
      } catch (Exception ex) {
        LOG.error("get task[" + taskID + "] from history_queue error", ex);
      }

      if (task == null) {
        return;
      }

      try {
        if (task.getState() == TaskState.Success) {
          sink.next(Boolean.TRUE);
          sink.complete();
        } else {
          sink.error(DBException.get(ErrorModule.DDL, ErrorCode.valueOf(task.getErrorCode()), false, task.getError()));
        }
      } catch (Exception ex) {
        LOG.error("reply task[" + taskID + "] error", ex);
      } finally {
        future.cancel(true);
      }
    }
  }
}
