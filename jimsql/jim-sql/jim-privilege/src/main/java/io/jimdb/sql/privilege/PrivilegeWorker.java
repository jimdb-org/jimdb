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
package io.jimdb.sql.privilege;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.PrivilegeOp;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @version V1.0
 */
final class PrivilegeWorker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeWorker.class);
  private static final int MAX_RETRY_NUM = 10;

  private final int delay;
  private final MetaStore metaStore;
  private final ScheduledExecutorService loadExecutor;
  private final AtomicBoolean running = new AtomicBoolean(false);

  PrivilegeWorker(MetaStore metaStore) {
    this.delay = 10000;
    this.metaStore = metaStore;
    this.loadExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Privilege-TaskWorker-Executor", true));
  }

  void start() {
    metaStore.watch(MetaStore.WatchType.PRITASK, l -> loadExecutor.execute(() -> loadTask()));
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

    OpType lockType = null;
    Task task = null;
    try {
      List<Task> tasks = metaStore.getTasks(TaskType.PRITASK, null);
      if (tasks == null || tasks.isEmpty()) {
        return;
      }

      for (Task t : tasks) {
        task = t;
        if (!metaStore.tryLock(MetaStore.TaskType.PRITASK, null)) {
          return;
        }

        lockType = task.getOp();
        task = metaStore.getTask(TaskType.PRITASK, task);
        if (task == null) {
          continue;
        }

        if (metaStore.getTask(TaskType.PRIHISTORY, task) != null) {
          metaStore.removeTask(TaskType.PRITASK, task.getId());
        } else {
          this.handleTask(task);
        }
      }
    } catch (Exception ex) {
      LOG.error(String.format("handle privilege task[%s] error", task == null ? "load tasks" : task.toString()), ex);
    } finally {
      running.compareAndSet(true, false);
      if (lockType != null) {
        metaStore.unLock(TaskType.PRITASK, null);
      }
    }
  }

  private void handleTask(Task task) {
    Task oldTask = task;
    while (true) {
      if (task.getState() == TaskState.Failed || task.getState() == TaskState.Success) {
        this.commitTask(task);
        return;
      }

      if (!metaStore.tryLock(MetaStore.TaskType.PRITASK, null)) {
        throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_INTERNAL_ERROR, "lost the privilege task worker lock");
      }

      if (task.getState() == TaskState.Init) {
        task = task.toBuilder()
                .setState(TaskState.Running)
                .build();
        if (!metaStore.storeTask(TaskType.PRITASK, oldTask, task)) {
          throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_INTERNAL_ERROR, String.format("Task[%d] occur concurrent process error", task.getId()));
        }
        oldTask = task;
      }

      try {
        if (task.getRetryNum() > MAX_RETRY_NUM) {
          task = task.toBuilder()
                  .setState(TaskState.Failed)
                  .build();
        } else {
          task = this.runTask(task);
        }
      } catch (Exception ex) {
        task = task.toBuilder()
                .setRetryNum(task.getRetryNum() + 1)
                .setErrorCode(ex instanceof JimException ? ((JimException) ex).getCode().name() : ErrorCode.ER_UNKNOWN_ERROR.name())
                .setError(ex.getMessage())
                .build();
        metaStore.storeTask(TaskType.PRITASK, oldTask, task);

        throw ex;
      }

      if (!metaStore.storeTask(MetaStore.TaskType.PRITASK, oldTask, task)) {
        throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_INTERNAL_ERROR, String.format("Task[%d] occur concurrent process error", task.getId()));
      }
      oldTask = task;
    }
  }

  private Task runTask(Task task) {
    if (task.getState() == TaskState.Failed || task.getState() == TaskState.Success) {
      return task;
    }

    String errorCode = "";
    String error = "";
    TaskState taskState = task.getState();
    try {
      PrivilegeOp op = PrivilegeOp.parseFrom(task.getData());
      switch (task.getOp()) {
        case PriGrant:
          if (TaskPrivHandler.grantPriv(op)) {
            taskState = TaskState.Success;
          }
          break;
        case PriRevoke:
          if (TaskPrivHandler.revokePriv(op)) {
            taskState = TaskState.Success;
          }
          break;
        case PriSetPassword:
          if (TaskPrivHandler.setPassword(op)) {
            taskState = TaskState.Success;
          }
          break;
        case PriCreateUser:
          if (TaskPrivHandler.createUser(op)) {
            taskState = TaskState.Success;
          }
          break;
        case PriUpdateUser:
          if (TaskPrivHandler.alterUser(op)) {
            taskState = TaskState.Success;
          }
          break;
        case PriDropUser:
          if (TaskPrivHandler.dropUser(op)) {
            taskState = TaskState.Success;
          }
          break;
        default:
          break;
      }
    } catch (InvalidProtocolBufferException ex) {
      errorCode = ErrorCode.ER_INTERNAL_ERROR.name();
      error = ex.getMessage();
      taskState = TaskState.Failed;
    } catch (PrivilegeException ex) {
      errorCode = ex.getCode().name();
      error = ex.getMessage();
      taskState = TaskState.Failed;
    }

    Task.Builder builder = task.toBuilder();
    builder.setState(taskState)
            .setErrorCode(errorCode)
            .setError(error)
            .setRetryNum(0);
    return builder.build();
  }

  private void commitTask(Task task) {
    LOG.warn("handle privilege task[{}] complete", task);

    metaStore.addAndGetPrivVersion(1);
    metaStore.storeTask(MetaStore.TaskType.PRIHISTORY, null, task);
    metaStore.removeTask(MetaStore.TaskType.PRITASK, task.getId());
  }
}
