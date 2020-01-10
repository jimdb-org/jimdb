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

import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.common.utils.lang.NamedThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCBL_FIELD_COULD_BE_LOCAL")
final class IndexWorker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexWorker.class);

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

        Ddlpb.IndexReorg reorg = Ddlpb.IndexReorg.parseFrom(task.getData());
        String key = task.getId() + "_" + reorg.getOffset();
        if (reorgWorkings.contains(key) || task.getState() == TaskState.Success) {
          continue;
        }

        Task indexStatus = metaStore.getTask(TaskType.INDEXTASK, Task.newBuilder().setId(task.getId()).build());
        if (indexStatus == null || indexStatus.getState() != TaskState.Running) {
          continue;
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
   *
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
        task = metaStore.getTask(TaskType.INDEXTASK, task);
        if (task == null || task.getState() == TaskState.Success) {
          return;
        }
      } catch (Exception ex) {
        LOG.error(String.format("execute index reorg '%s' error", key), ex);
      } finally {
        workings.remove(key);
      }
    }
  }
}
