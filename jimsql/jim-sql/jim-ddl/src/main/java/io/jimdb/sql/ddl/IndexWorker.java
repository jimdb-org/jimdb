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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.lang.NamedThreadFactory;
import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.pb.Ddlpb.IndexReorg;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "FCBL_FIELD_COULD_BE_LOCAL" })
final class IndexWorker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexWorker.class);

  private static final int BATCH_SIZE = 100;
  private static final int RETRY_MAX = 10;

  private final int delay;
  private final int reorgThreads;
  private final MetaStore metaStore;
  private final Engine storeEngine;
  private final ScheduledExecutorService loadExecutor;
  private final ExecutorService reorgExecutor;
  private final CopyOnWriteArrayList<String> reorgWorkings = new CopyOnWriteArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);

  IndexWorker(MetaStore metaStore, Engine engine, int threads) {
    this.delay = 10000;
    this.reorgThreads = threads;
    this.metaStore = metaStore;
    this.storeEngine = engine;
    this.loadExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("IndexReorg-TaskLoader-Executor", true));
    this.reorgExecutor = new ThreadPoolExecutor(0, this.reorgThreads, 10L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), new NamedThreadFactory("IndexReorg-Worker-Executor", true));
  }

  void start() {
    metaStore.watch(MetaStore.WatchType.INDEXTASK, l -> loadExecutor.execute(() -> loadTask()));
    loadExecutor.scheduleWithFixedDelay(() -> loadTask(), delay, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (loadExecutor != null) {
      loadExecutor.shutdown();
    }
    if (reorgExecutor != null) {
      reorgExecutor.shutdown();
    }
  }

  private void loadTask() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    try {
      if (reorgWorkings.size() >= reorgThreads) {
        return;
      }

      List<Task> tasks = metaStore.getTasks(TaskType.INDEXTASK, null);
      if (tasks == null || tasks.isEmpty()) {
        return;
      }

      for (Task task : tasks) {
        if (reorgWorkings.size() >= reorgThreads) {
          return;
        }

        IndexReorg reorg = IndexReorg.parseFrom(task.getData());
        String key = task.getId() + "_" + reorg.getOffset();
        if (reorgWorkings.contains(key) || task.getState() == TaskState.Success) {
          continue;
        }

        Task taskStatus = metaStore.getTask(TaskType.INDEXTASK, Task.newBuilder().setId(task.getId()).build());
        if (taskStatus == null || taskStatus.getState() != TaskState.Running) {
          return;
        }
        if (!metaStore.tryLock(TaskType.INDEXTASK, task)) {
          continue;
        }

        reorgWorkings.addIfAbsent(key);
        reorgExecutor.execute(new ReorgWorker(key, task, reorgWorkings, metaStore, storeEngine));
      }
    } catch (Exception ex) {
      LOG.error("load index reorg tasks error", ex);
    } finally {
      running.compareAndSet(true, false);
    }
  }

  /**
   * TODO
   */
  static final class ReorgWorker implements Runnable {
    private final String key;
    private final MetaStore metaStore;
    private final Engine storeEngine;
    private final List<String> workings;
    private Task task;

    ReorgWorker(String key, Task task, List<String> workings, MetaStore metaStore, Engine storeEngine) {
      this.key = key;
      this.task = task;
      this.workings = workings;
      this.metaStore = metaStore;
      this.storeEngine = storeEngine;
    }

    @Override
    public void run() {
      try {
        int count = 0;
        Task taskStatus;
        Session session = new Session(PluginFactory.getSqlEngine(), storeEngine);

        while (true) {
          if (task.getState() == TaskState.Success) {
            return;
          }

          taskStatus = metaStore.getTask(TaskType.INDEXTASK, Task.newBuilder().setId(task.getId()).build());
          if (taskStatus == null || taskStatus.getState() != TaskState.Running) {
            return;
          }
          if (task.getRetryNum() > RETRY_MAX) {
            cancelIndexTask(taskStatus, task.getErrorCode(), task.getError());
            return;
          }

          if (!metaStore.tryLock(TaskType.INDEXTASK, task)) {
            return;
          }

          if (doReorg(session, taskStatus)) {
            if (++count % 5 == 0) {
              Thread.sleep(200);
            }
          } else {
            Thread.sleep(3000);
          }
        }
      } catch (Exception ex) {
        LOG.error(String.format("execute index reorg '%s' error", key), ex);
      } finally {
        workings.remove(key);
      }
    }

    private boolean doReorg(Session session, Task taskStatus) {
      String error = "";
      String errorCode = "";
      boolean isSuccess = true;
      IndexReorg reorg = null;

      try {
        Index index = null;
        reorg = IndexReorg.parseFrom(task.getData());
        Table table = session.getTxnContext().getMetaData().getTable(task.getDbId(), task.getTableId());
        if (table != null) {
          for (Index idx : table.getWritableIndices()) {
            if (idx.getId().intValue() == reorg.getIndexId()) {
              index = idx;
              break;
            }
          }
        }
        if (index == null) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", task.getTableId()));
        }

        Transaction txn = session.getTxn();
        byte[] lastKey = storeEngine.addIndex(session, index, NettyByteString.asByteArray(reorg.getLastKey()),
                NettyByteString.asByteArray(reorg.getEndKey()), BATCH_SIZE);
        txn.commit().blockFirst();

        reorg = reorg.toBuilder()
                .setLastKey(NettyByteString.wrap(lastKey))
                .build();
      } catch (InvalidProtocolBufferException ex) {
        DDLException e = new DDLException(DDLException.ErrorType.FAILED, ex);
        cancelIndexTask(taskStatus, e.getCode().name(), e.getMessage());
        return isSuccess;
      } catch (DDLException ex) {
        throw ex;
      } catch (BaseException ex) {
        isSuccess = false;
        if (ex.getCode() != ErrorCode.ER_TXN_CONFLICT && ex.getCode() != ErrorCode.ER_TXN_VERSION_CONFLICT
                && ex.getCode() != ErrorCode.ER_TXN_STATUS_CONFLICT) {
          errorCode = ex.getCode().name();
          error = ex.getMessage();
        }
      }

      if (reorg != null) {
        updateTask(reorg, errorCode, error);
      }
      return isSuccess;
    }

    private void updateTask(IndexReorg reorg, String errorCode, String error) {
      Task oldTask = task;
      int retryNum = 0;
      if (StringUtils.isNotBlank(errorCode)) {
        retryNum = task.getRetryNum() + 1;
      }

      TaskState state = task.getState();
      if (ByteUtil.compare(reorg.getLastKey(), reorg.getEndKey()) >= 0) {
        state = TaskState.Success;
      }

      task = task.toBuilder()
              .setData(reorg.toByteString())
              .setRetryNum(retryNum)
              .setError(error)
              .setErrorCode(errorCode)
              .setState(state)
              .build();
      if (!metaStore.storeTask(TaskType.INDEXTASK, oldTask, task)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", task.getTableId()));
      }
    }

    private void cancelIndexTask(Task taskStatus, String errorCode, String error) {
      Task oldTask = taskStatus;
      taskStatus = taskStatus.toBuilder()
              .setError(error)
              .setErrorCode(errorCode)
              .setState(TaskState.Failed)
              .build();
      if (!metaStore.storeTask(TaskType.INDEXTASK, oldTask, taskStatus)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", task.getTableId()));
      }
    }
  }
}
