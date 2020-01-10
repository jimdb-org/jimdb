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
package reactor.core.scheduler;

import static reactor.core.Exceptions.unwrap;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * @version V1.0
 */
public final class JimExecutorScheduler implements Scheduler, Scannable, Closeable {
  private static final Logger LOG = Loggers.getLogger(JimExecutorScheduler.class);

  private static final BooleanSupplier NO_PARENT = () -> false;
  private static volatile BiConsumer<Thread, ? super Throwable> onHandleErrorHook;

  static void handleError(Throwable ex) {
    Thread thread = Thread.currentThread();
    Throwable t = unwrap(ex);
    Exceptions.throwIfJvmFatal(t);
    Thread.UncaughtExceptionHandler x = thread.getUncaughtExceptionHandler();
    if (x != null) {
      x.uncaughtException(thread, t);
    } else {
      LOG.error("Scheduler worker failed with an uncaught exception", t);
    }
    if (onHandleErrorHook != null) {
      onHandleErrorHook.accept(thread, t);
    }
  }

  /**
   * Define a hook that is executed when a Scheduler has
   * {@link #handleError(Throwable) handled an error}. Note that it is executed after
   * the error has been passed to the thread uncaughtErrorHandler, which is not the
   * case when a fatal error occurs (see {@link Exceptions#throwIfJvmFatal(Throwable)}).
   *
   * @param c the new hook to set.
   */
  public static void onHandleError(BiConsumer<Thread, ? super Throwable> c) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Hooking new: onHandleError");
    }
    onHandleErrorHook = Objects.requireNonNull(c, "onHandleError");
  }

  /**
   * Reset the {@link #onHandleError(BiConsumer)} hook to the default no-op behavior.
   */
  public static void resetOnHandleError() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Reset to default: onHandleError");
    }
    onHandleErrorHook = null;
  }

  private final ExecutorService pool;
  private final Scheduler scheduler;
  private final boolean disposeScheduler;

  public JimExecutorScheduler(final ExecutorService pool, final int schedulerParallel) {
    this.pool = pool;
    this.scheduler = new ParallelScheduler(schedulerParallel,
            new ReactorThreadFactory("ForkJoinPoolScheduler-timer", ParallelScheduler.COUNTER, true,
                    true, Schedulers::defaultUncaughtException));
    this.disposeScheduler = true;
  }

  @Override
  public Scheduler.Worker createWorker() {
    return new Worker(pool, scheduler);
  }

  @Override
  public void close() {
    if (disposeScheduler) {
      scheduler.dispose();
    }
  }

  @Override
  public void dispose() {
  }

  @Override
  public boolean isDisposed() {
    return pool.isShutdown();
  }

  @Override
  public Disposable schedule(Runnable runnable) {
    return new DisposableForkJoinTask(pool.submit(runnable));
  }

  @Override
  public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
    if (delay == 0) {
      return schedule(task);
    }
    TrampolinedTask trampolinedTask = new TrampolinedTask(pool, task, NO_PARENT);
    return new CompositeDisposable(scheduler.schedule(trampolinedTask, delay, unit), trampolinedTask);
  }

  @Override
  public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
    TrampolinedTask trampolinedTask = new TrampolinedTask(pool, task, NO_PARENT);
    return new CompositeDisposable(scheduler.schedulePeriodically(trampolinedTask,
            initialDelay, period, unit), trampolinedTask);
  }

  @Override
  @SuppressFBWarnings("URV_INHERITED_METHOD_WITH_RELATED_TYPES")
  public Object scanUnsafe(Attr key) {
    if (key == Attr.TERMINATED || key == Attr.CANCELLED) {
      return isDisposed();
    }
    if (key == Attr.NAME) {
      return "ForkJoinPoolScheduler(" + pool + ")";
    }

    return pool;
  }

  /**
   *
   */
  private static final class DisposableForkJoinTask implements Disposable {
    private final Future<?> task;

    DisposableForkJoinTask(Future<?> task) {
      this.task = task;
    }

    @Override
    public void dispose() {
      task.cancel(false);
    }

    @Override
    public boolean isDisposed() {
      return task.isDone();
    }
  }

  /**
   *
   */
  private static final class TrampolinedTask implements Runnable, Disposable {
    private final Executor executor;
    private final Runnable task;
    private final BooleanSupplier isParentDisposed;
    private volatile boolean disposed;

    TrampolinedTask(Executor executor, Runnable task, BooleanSupplier isParentDisposed) {
      this.executor = executor;
      this.task = task;
      this.isParentDisposed = isParentDisposed;
    }

    @Override
    public void dispose() {
      disposed = true;
    }

    @Override
    public boolean isDisposed() {
      return disposed || isParentDisposed.getAsBoolean();
    }

    @Override
    public void run() {
      executor.execute(() -> {
        if (!isDisposed()) {
          task.run();
        }
      });
    }
  }

  /**
   *
   */
  private static final class CompositeDisposable implements Disposable {
    final Disposable a;
    final Disposable b;

    CompositeDisposable(Disposable a, Disposable b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public void dispose() {
      a.dispose();
      b.dispose();
    }

    @Override
    public boolean isDisposed() {
      return a.isDisposed() && b.isDisposed();
    }
  }

  /**
   *
   */
  private static final class Worker implements Scheduler.Worker {
    private final Executor executor;
    private final Scheduler scheduler;
    private final Lock lock = new ReentrantLock();
    private final Queue<Runnable> tasks = new ArrayDeque<>();
    private boolean executing = false;
    private final Runnable processTaskQueue = this::processTaskQueue;
    private final Executor workerExecutor = this::execute;
    private volatile boolean shutdown;
    private final BooleanSupplier isDisposed = this::isDisposed;

    Worker(Executor executor, Scheduler scheduler) {
      this.executor = executor;
      this.scheduler = scheduler;
    }

    @Override
    public void dispose() {
      if (shutdown) {
        return;
      }

      shutdown = true;
      lock.lock();
      try {
        tasks.clear();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isDisposed() {
      return shutdown;
    }

    @Override
    public Disposable schedule(Runnable task) {
      if (shutdown) {
        throw Exceptions.failWithRejected();
      }

      DisposableWorkerTask workerTask = new DisposableWorkerTask(task, isDisposed);
      try {
        execute(workerTask);
      } catch (RejectedExecutionException ignored) {
        // dispose the task since it made it into the queue
        workerTask.dispose();
        throw ignored;
      }

      return workerTask;
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
      if (delay == 0) {
        return schedule(task);
      }

      if (shutdown) {
        throw Exceptions.failWithRejected();
      }

      TrampolinedTask trampolinedTask = new TrampolinedTask(workerExecutor, task, isDisposed);
      return new CompositeDisposable(scheduler.schedule(trampolinedTask, delay, unit), trampolinedTask);
    }

    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
      if (shutdown) {
        throw Exceptions.failWithRejected();
      }

      TrampolinedTask trampolinedTask = new TrampolinedTask(workerExecutor, task, isDisposed);

      return new CompositeDisposable(scheduler.schedulePeriodically(trampolinedTask,
              initialDelay, period, unit), trampolinedTask);
    }

    private void execute(Runnable command) {
      boolean schedule;
      lock.lock();
      try {
        tasks.add(command);
        if (executing) {
          schedule = false;
        } else {
          executing = true;
          schedule = true;
        }
      } finally {
        lock.unlock();
      }

      if (schedule) {
        executor.execute(processTaskQueue);
      }
    }

    private void processTaskQueue() {
      while (true) {
        Runnable task;
        lock.lock();
        try {
          task = tasks.poll();
          if (task == null) {
            executing = false;
            return;
          }
        } finally {
          lock.unlock();
        }

        try {
          task.run();
        } catch (Throwable ex) {
          handleError(ex);
        }
      }
    }
  }

  /**
   *
   */
  private static final class DisposableWorkerTask implements Disposable, Runnable {
    private final Runnable task;
    private final BooleanSupplier isParentDisposed;
    private volatile boolean disposed;

    private DisposableWorkerTask(Runnable task, BooleanSupplier disposed) {
      this.task = task;
      isParentDisposed = disposed;
    }

    @Override
    public void dispose() {
      disposed = true;
    }

    @Override
    public boolean isDisposed() {
      return disposed || isParentDisposed.getAsBoolean();
    }

    @Override
    public void run() {
      if (isDisposed()) {
        return;
      }

      task.run();
    }
  }
}
