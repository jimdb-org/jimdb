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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.core.plugin.MetaStore.WatchType;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Ddlpb.AlterTableInfo;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

/**
 * Responsible for performing DDL Create tasks.
 *
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
final class DDLWorker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DDLWorker.class);
  private static final int MAX_RETRY_NUM = 10;

  private final int delay;
  private final MetaStore metaStore;
  private final RouterStore routerStore;
  private final Engine storeEngine;
  private final DDLSyncer ddlSyncer;
  private final ScheduledExecutorService loadExecutor;
  private final AtomicBoolean running = new AtomicBoolean(false);

  DDLWorker(MetaStore metaStore, RouterStore routerStore, Engine engine, DDLSyncer ddlSyncer) {
    this.delay = 10000;
    this.metaStore = metaStore;
    this.storeEngine = engine;
    this.routerStore = routerStore;
    this.ddlSyncer = ddlSyncer;
    this.loadExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("DDL-TaskWorker-Executor", true));
  }

  void start() {
    metaStore.watch(WatchType.SCHEMATASK, l -> loadExecutor.execute(() -> loadTask()));
    loadExecutor.scheduleWithFixedDelay(() -> loadTask(), delay, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (loadExecutor != null) {
      loadExecutor.shutdown();
    }
  }

  private void loadTask() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    int lockTable = -1;
    OpType lockType = null;
    Task task = null;
    List<Integer> failedLocks = new ArrayList<>(8);
    Map<String, Integer> catalogTasks = new HashMap<>();
    Map<String, Integer> tableTasks = new HashMap<>();
    try {
      List<Task> tasks = metaStore.getTasks(TaskType.SCHEMATASK, null);
      if (tasks == null || tasks.isEmpty()) {
        return;
      }

      boolean init = true;
      for (Task t : tasks) {
        Tuple3<Task, String, String> verifys = verifyTask(t);
        if (!handleBefore(verifys, catalogTasks, tableTasks)) {
          continue;
        }

        task = verifys.getT1();
        if (task.getState() != TaskState.Failed && task.getState() != TaskState.Success) {
          int dbID = task.getDbId();
          int tableID = MetaStore.isGlobalTask(task.getOp()) ? 0 : task.getTableId();
          if (failedLocks.contains(tableID)) {
            continue;
          }

          if (lockTable != -1 && lockTable != tableID) {
            metaStore.unLock(TaskType.SCHEMATASK, Task.newBuilder()
                    .setOp(lockType).setTableId(lockTable).build());
            lockTable = -1;
            lockType = null;
          }

          if (!metaStore.tryLock(TaskType.SCHEMATASK, task)) {
            failedLocks.add(tableID);
            continue;
          }

          lockTable = tableID;
          lockType = task.getOp();
          tableID = task.getTableId();
          task = metaStore.getTask(TaskType.SCHEMATASK, task);
          if (task != null && task.getDbId() <= 0) {
            Task oldTask = task;
            task = task.toBuilder()
                    .setDbId(dbID)
                    .setTableId(tableID)
                    .build();
            if (!metaStore.storeTask(TaskType.SCHEMATASK, oldTask, task)) {
              throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Task[%d] occur concurrent process error", task.getId()));
            }
          }
        }

        if (task != null) {
          if (metaStore.getTask(TaskType.SCHEMAHISTORY, task) != null) {
            metaStore.removeTask(TaskType.SCHEMATASK, task.getId());
          } else {
            if (init && task.getState() != TaskState.Failed && task.getState() != TaskState.Success) {
              init = false;
              metaStore.addAndGetVersion(1);
            }

            this.handleTask(task);
          }
        }

        handlePost(verifys, catalogTasks, tableTasks);
      }
    } catch (Exception ex) {
      LOG.error(String.format("handle task[%s] error", task == null ? "load tasks" : task.toString()), ex);
    } finally {
      running.compareAndSet(true, false);
      if (lockTable != -1) {
        metaStore.unLock(TaskType.SCHEMATASK, Task.newBuilder()
                .setOp(lockType).setTableId(lockTable).build());
      }
    }
  }

  private void handleTask(Task task) {
    Task oldTask = task;
    long expectVer = metaStore.addAndGetVersion(0);
    while (true) {
      ddlSyncer.waitMetaSynced(expectVer);
      if (task.getState() == TaskState.Failed || task.getState() == TaskState.Success) {
        this.commitTask(task);
        return;
      }

      if (!metaStore.tryLock(TaskType.SCHEMATASK, task)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, "lost the ddl task worker lock");
      }

      if (task.getState() == TaskState.Init) {
        task = task.toBuilder()
                .setState(TaskState.Running)
                .build();
        if (!metaStore.storeTask(TaskType.SCHEMATASK, oldTask, task)) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Task[%d] occur concurrent process error", task.getId()));
        }
        oldTask = task;
      }

      try {
        if (task.getState() == TaskState.Rollingback || task.getRetryNum() > MAX_RETRY_NUM) {
          if (task.getState() != TaskState.Rollingback) {
            task = task.toBuilder()
                    .setState(TaskState.Rollingback)
                    .build();
            if (!metaStore.storeTask(TaskType.SCHEMATASK, oldTask, task)) {
              throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Task[%d] occur concurrent process error", task.getId()));
            }
            oldTask = task;
          }
          task = this.rollbackTask(task);
        } else {
          task = this.runTask(task);
        }
      } catch (DDLException ex) {
        throw ex;
      } catch (Exception ex) {
        if (task.getState() != TaskState.Rollingback) {
          task = task.toBuilder()
                  .setRetryNum(task.getRetryNum() + 1)
                  .setErrorCode(ex instanceof JimException ? ((JimException) ex).getCode().name() : ErrorCode.ER_UNKNOWN_ERROR.name())
                  .setError(ex.getMessage())
                  .build();
          metaStore.storeTask(TaskType.SCHEMATASK, oldTask, task);
        }

        throw ex;
      }

      expectVer = task.getMetaVersion();
      if (!metaStore.storeTask(TaskType.SCHEMATASK, oldTask, task)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Task[%d] occur concurrent process error", task.getId()));
      }
      oldTask = task;
    }
  }

  @SuppressFBWarnings({ "CC_CYCLOMATIC_COMPLEXITY", "NP_NULL_ON_SOME_PATH" })
  private Task runTask(Task task) {
    if (task.getState() == TaskState.Failed || task.getState() == TaskState.Success) {
      return task;
    }

    String errorCode = "";
    String error = "";
    String tableName = "";
    OpType opType;
    ByteString arg = null;
    AlterTableInfo alterInfo = null;
    MetaState metaState = task.getMetaState();
    TaskState taskState = task.getState();
    try {
      switch (task.getOp()) {
        case AlterTable:
          alterInfo = AlterTableInfo.parseFrom(task.getData());
          opType = alterInfo.getType();
          arg = alterInfo.getItem();
          tableName = alterInfo.getTableName();
          break;
        default:
          opType = task.getOp();
          arg = task.getData();
      }

      switch (opType) {
        case CreateCatalog:
          CatalogInfo catalog = CatalogInfo.parseFrom(arg);
          catalog = TaskDBHandler.createCatalog(metaStore, catalog);
          metaState = catalog.getState();
          arg = catalog.toByteString();
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case DropCatalog:
          metaState = TaskDBHandler.dropCatalog(metaStore, routerStore, task.getDbId());
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case CreateTable:
          TableInfo table = TableInfo.parseFrom(arg);
          table = TaskTableHandler.createTable(metaStore, routerStore, task.getDbId(), table);
          metaState = table.getState();
          arg = table.toByteString();
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case DropTable:
          metaState = TaskTableHandler.dropTable(metaStore, routerStore, task.getDbId(), task.getTableId());
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case AddIndex:
          TableInfo addIndexTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (addIndexTable == null || addIndexTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          Ddlpb.AddIndexInfo index = Ddlpb.AddIndexInfo.parseFrom(arg);
          index = TaskIndexHandler.addIndex(metaStore, routerStore, storeEngine, task.getId(), addIndexTable, index);
          metaState = index.getState();
          arg = index.toByteString();
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case DropIndex:
          TableInfo dropIndexTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (dropIndexTable == null || dropIndexTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          IndexInfo indexInfo = IndexInfo.parseFrom(arg);
          indexInfo = TaskIndexHandler.dropIndex(metaStore, routerStore, dropIndexTable, indexInfo);
          metaState = indexInfo.getState();
          arg = indexInfo.toByteString();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case AddColumn:
          TableInfo addColumnTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (addColumnTable == null || addColumnTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          Ddlpb.AddColumnInfo addColumn = Ddlpb.AddColumnInfo.parseFrom(arg);
          addColumn = TaskColumnHandler.addColumn(metaStore, addColumnTable, addColumn);
          metaState = addColumn.getState();
          arg = addColumn.toByteString();
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case DropColumn:
          TableInfo dropColumnTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (dropColumnTable == null || dropColumnTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          ColumnInfo dropColumn = ColumnInfo.parseFrom(arg);
          dropColumn = TaskColumnHandler.dropColumn(metaStore, dropColumnTable, dropColumn);
          metaState = dropColumn.getState();
          arg = dropColumn.toByteString();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case RenameTable:
          TableInfo renameTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (renameTable == null || renameTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          TableInfo toTable = TableInfo.parseFrom(arg);
          metaState = TaskTableHandler.renameTable(metaStore, renameTable, toTable.getName());
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case RenameIndex:
          TableInfo renameIndexTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (renameIndexTable == null || renameIndexTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          IndexInfo renameIndex = IndexInfo.parseFrom(arg);
          metaState = TaskIndexHandler.renameIndex(metaStore, renameIndexTable, renameIndex.getName(), renameIndex.getComment());
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        case AlterAutoInitId:
          TableInfo autoTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (autoTable == null || autoTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, tableName);
          }

          metaState = TaskTableHandler.alterAutoInitId(metaStore, autoTable, Long.parseLong(arg.toStringUtf8()));
          if (metaState == MetaState.Public) {
            taskState = TaskState.Success;
          }
          break;

        default:
          errorCode = ErrorCode.ER_NOT_SUPPORTED_YET.name();
          error = JimException.message(ErrorCode.ER_NOT_SUPPORTED_YET, task.getOp().name());
          taskState = TaskState.Failed;
      }
    } catch (InvalidProtocolBufferException ex) {
      errorCode = ErrorCode.ER_INTERNAL_ERROR.name();
      error = ex.getMessage();
      taskState = TaskState.Rollingback;
    } catch (DDLException ex) {
      if (ex.type == DDLException.ErrorType.CONCURRENT) {
        throw ex;
      }
      errorCode = ex.getCode().name();
      error = ex.getMessage();
      taskState = ex.type == DDLException.ErrorType.FAILED ? TaskState.Failed : TaskState.Rollingback;
    }

    if (alterInfo != null) {
      arg = alterInfo.toBuilder()
              .setItem(arg)
              .build()
              .toByteString();
    }
    Task.Builder builder = task.toBuilder();
    builder.setMetaVersion(metaStore.addAndGetVersion(1))
            .setData(arg)
            .setMetaState(metaState)
            .setState(taskState)
            .setErrorCode(errorCode)
            .setError(error)
            .setRetryNum(0);
    return builder.build();
  }

  private Task rollbackTask(Task task) {
    OpType opType;
    ByteString arg = null;
    AlterTableInfo alterInfo = null;
    MetaState metaState = task.getMetaState();
    TaskState taskState = task.getState();
    try {
      switch (task.getOp()) {
        case AlterTable:
          alterInfo = AlterTableInfo.parseFrom(task.getData());
          opType = alterInfo.getType();
          arg = alterInfo.getItem();
          break;
        default:
          opType = task.getOp();
          arg = task.getData();
      }

      switch (opType) {
        case CreateCatalog:
          metaState = MetaState.Absent;
          taskState = TaskState.Failed;
          metaStore.removeCatalog(task.getDbId());
          break;
        case DropCatalog:
          metaState = TaskDBHandler.dropCatalog(metaStore, routerStore, task.getDbId());
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;
        case CreateTable:
          metaState = TaskTableHandler.dropTable(metaStore, routerStore, task.getDbId(), task.getTableId());
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Failed;
          }
          break;
        case DropTable:
          metaState = TaskTableHandler.dropTable(metaStore, routerStore, task.getDbId(), task.getTableId());
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case AddIndex:
          metaStore.removeTask(TaskType.INDEXTASK, task.getId());

          TableInfo addIndexTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (addIndexTable == null || addIndexTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(task.getTableId()));
          }

          Ddlpb.AddIndexInfo addIndexInfo = Ddlpb.AddIndexInfo.parseFrom(arg);
          IndexInfo index = IndexInfo.newBuilder()
                  .setId(addIndexInfo.getId())
                  .setName(addIndexInfo.getName())
                  .build();
          index = TaskIndexHandler.dropIndex(metaStore, routerStore, addIndexTable, index);
          metaState = index.getState();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Failed;
          }
          break;

        case DropIndex:
          TableInfo dropIndexTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (dropIndexTable == null || dropIndexTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(task.getTableId()));
          }

          IndexInfo indexInfo = IndexInfo.parseFrom(arg);
          indexInfo = TaskIndexHandler.dropIndex(metaStore, routerStore, dropIndexTable, indexInfo);
          metaState = indexInfo.getState();
          arg = indexInfo.toByteString();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        case AddColumn:
          TableInfo addColumnTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (addColumnTable == null || addColumnTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(task.getTableId()));
          }

          ColumnInfo addColumn = ColumnInfo.newBuilder()
                  .setName(Ddlpb.AddColumnInfo.parseFrom(arg).getName())
                  .build();
          addColumn = TaskColumnHandler.dropColumn(metaStore, addColumnTable, addColumn);
          metaState = addColumn.getState();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Failed;
          }
          break;

        case DropColumn:
          TableInfo dropColumnTable = metaStore.getTable(task.getDbId(), task.getTableId());
          if (dropColumnTable == null || dropColumnTable.getState() != MetaState.Public) {
            throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(task.getTableId()));
          }

          ColumnInfo dropColumn = ColumnInfo.parseFrom(arg);
          dropColumn = TaskColumnHandler.dropColumn(metaStore, dropColumnTable, dropColumn);
          metaState = dropColumn.getState();
          arg = dropColumn.toByteString();
          if (metaState == MetaState.Absent) {
            taskState = TaskState.Success;
          }
          break;

        default:
          taskState = TaskState.Failed;
          break;
      }
    } catch (InvalidProtocolBufferException ex) {
      taskState = TaskState.Failed;
    } catch (DDLException ex) {
      if (ex.type == DDLException.ErrorType.CONCURRENT) {
        throw ex;
      }
      taskState = ex.type == DDLException.ErrorType.FAILED ? TaskState.Failed : TaskState.Rollingback;
    }

    if (alterInfo != null) {
      arg = alterInfo.toBuilder()
              .setItem(arg)
              .build()
              .toByteString();
    }
    Task.Builder builder = task.toBuilder();
    builder.setMetaVersion(metaStore.addAndGetVersion(1))
            .setData(arg)
            .setMetaState(metaState)
            .setState(taskState);
    return builder.build();
  }

  private void commitTask(Task task) {
    LOG.warn("handle task[{}] complete", task);

    metaStore.storeTask(TaskType.SCHEMAHISTORY, null, task);
    metaStore.removeTask(TaskType.SCHEMATASK, task.getId());
  }

  private Tuple3<Task, String, String> verifyTask(Task task) {
    if (task.getState() == TaskState.Failed || task.getState() == TaskState.Success) {
      return Tuples.of(task, "", "");
    }

    int dbID = task.getDbId();
    int tableID = task.getTableId();
    String dbName = "";
    String tableName = "";
    String error = null;
    ErrorCode errorCode = null;
    try {
      switch (task.getOp()) {
        case CreateCatalog:
          CatalogInfo catalog = CatalogInfo.parseFrom(task.getData().toByteArray());
          dbID = catalog.getId();
          dbName = catalog.getName();
          break;
        case CreateTable:
          TableInfo table = TableInfo.parseFrom(task.getData().toByteArray());
          tableID = table.getId();
          dbName = DDLUtils.splitTableName(table.getName()).getT1();
          tableName = DDLUtils.splitTableName(table.getName()).getT2();
          break;
        case DropCatalog:
          dbName = CatalogInfo.parseFrom(task.getData().toByteArray()).getName();
          break;
        case AlterTable:
          AlterTableInfo alterInfo = AlterTableInfo.parseFrom(task.getData().toByteArray());
          dbName = alterInfo.getDbName();
          tableName = alterInfo.getTableName();
          break;
        default:
          error = "UnSupport ddl task type(" + task.getOp().name() + ")";
          errorCode = ErrorCode.ER_INTERNAL_ERROR;
      }
    } catch (InvalidProtocolBufferException ex) {
      error = ex.getMessage();
      errorCode = ErrorCode.ER_INTERNAL_ERROR;
    }

    if (error == null && dbID <= 0) {
      List<CatalogInfo> catalogs = metaStore.getCatalogs();
      if (catalogs != null) {
        for (CatalogInfo catalog : catalogs) {
          if (catalog.getState() == MetaState.Public && catalog.getName().equalsIgnoreCase(dbName)) {
            dbID = catalog.getId();
            break;
          }
        }
      }

      if (dbID == 0) {
        errorCode = ErrorCode.ER_BAD_DB_ERROR;
        error = DBException.message(errorCode, dbName);
      } else if (StringUtils.isNotBlank(tableName)) {
        List<TableInfo> tables = metaStore.getTables(dbID);
        if (tables != null) {
          for (TableInfo table : tables) {
            if (table.getState() == MetaState.Public && table.getName().equalsIgnoreCase(tableName)) {
              tableID = table.getId();
              break;
            }
          }
        }

        if (tableID == 0) {
          errorCode = ErrorCode.ER_BAD_TABLE_ERROR;
          error = DBException.message(errorCode, tableName);
        }
      }
    }

    Task.Builder builder = task.toBuilder();
    if (error == null) {
      builder.setDbId(dbID)
              .setTableId(tableID);
    } else {
      builder.setState(TaskState.Failed)
              .setErrorCode(errorCode.name())
              .setError(error);
    }
    return Tuples.of(builder.build(), dbName, tableName);
  }

  @SuppressFBWarnings("BX_UNBOXING_IMMEDIATELY_REBOXED")
  private boolean handleBefore(Tuple3<Task, String, String> taskInfo, Map<String, Integer> catalogTasks, Map<String, Integer> tableTasks) {
    Task task = taskInfo.getT1();
    String dbName = taskInfo.getT2();
    String tableName = taskInfo.getT3();
    if (StringUtils.isBlank(dbName)) {
      return true;
    }

    boolean result = true;
    Integer catalogCount = catalogTasks.get(dbName);
    Integer tableCount = tableTasks.get(tableName);
    catalogCount = catalogCount == null ? 0 : catalogCount;
    tableCount = tableCount == null ? 0 : tableCount;
    switch (task.getOp()) {
      case CreateCatalog:
      case DropCatalog:
        if (catalogCount > 0) {
          result = false;
        }
        catalogCount = catalogCount + 1;
        break;

      default:
        if (catalogCount > 0 || tableCount > 0) {
          result = false;
        }
        catalogCount = catalogCount + 1;
        tableCount = tableCount + 1;
        break;
    }

    if (StringUtils.isNotBlank(dbName)) {
      catalogTasks.put(dbName, catalogCount);
    }
    if (StringUtils.isNotBlank(tableName)) {
      tableTasks.put(tableName, tableCount);
    }
    return result;
  }

  @SuppressFBWarnings("BX_UNBOXING_IMMEDIATELY_REBOXED")
  private void handlePost(Tuple3<Task, String, String> taskInfo, Map<String, Integer> catalogTasks, Map<String, Integer> tableTasks) {
    Task task = taskInfo.getT1();
    String dbName = taskInfo.getT2();
    String tableName = taskInfo.getT3();
    if (StringUtils.isBlank(dbName)) {
      return;
    }

    Integer catalogCount = catalogTasks.get(dbName);
    Integer tableCount = tableTasks.get(tableName);
    catalogCount = catalogCount == null ? 1 : catalogCount;
    tableCount = tableCount == null ? 1 : tableCount;
    switch (task.getOp()) {
      case CreateCatalog:
      case DropCatalog:
        catalogCount = catalogCount - 1;
        break;

      default:
        catalogCount = catalogCount - 1;
        tableCount = tableCount - 1;
        break;
    }

    if (StringUtils.isNotBlank(dbName)) {
      catalogTasks.put(dbName, catalogCount);
    }
    if (StringUtils.isNotBlank(tableName)) {
      tableTasks.put(tableName, tableCount);
    }
  }
}
