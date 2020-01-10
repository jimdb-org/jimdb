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
package io.jimdb.core;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
public abstract class Service {
  protected volatile boolean started = false;

  protected volatile ServiceState state = ServiceState.STOPPED;

  protected final Object signal = new Object();

  protected final Lock lock = new ReentrantLock();

  public final void start() {
    lock.lock();
    try {
      if (started) {
        return;
      }

      state = ServiceState.WILL_START;
      started = true;
      beforeStart();
      state = ServiceState.STARTING;
      doStart();
      afterStart();
      state = ServiceState.STARTED;
    } catch (Exception ex) {
      state = ServiceState.START_FAILED;
      startError(ex);
      stop();
    } finally {
      lock.unlock();
    }
  }

  protected void beforeStart() {
  }

  protected void doStart() throws Exception {
  }

  protected void afterStart() {
  }

  protected void startError(Exception ex) {
  }

  public final void stop() {
    lock.lock();

    try {
      if (!started) {
        return;
      }

      state = ServiceState.WILL_STOP;
      beforeStop();
      state = ServiceState.STOPPING;
      doStop();
      afterStop();
      state = ServiceState.STOPPED;
    } finally {
      started = false;
      lock.unlock();
    }

    synchronized (signal) {
      signal.notifyAll();
    }
  }

  protected void beforeStop() {
  }

  protected void doStop() {
  }

  protected void afterStop() {
  }

  public ServiceState getState() {
    return state;
  }

  public boolean isStarted() {
    if (started) {
      switch (state) {
        case WILL_STOP:
          return false;
        case STOPPING:
          return false;
        case STOPPED:
          return false;
        default:
          return true;
      }
    }
    return false;
  }

  public boolean isStopped() {
    switch (state) {
      case START_FAILED:
        return true;
      case WILL_STOP:
        return true;
      case STOPPING:
        return true;
      case STOPPED:
        return true;
      default:
        return false;
    }
  }

  public void await(final long time) {
    if (!isStarted()) {
      return;
    }

    synchronized (signal) {
      try {
        signal.wait(time);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isReady() {
    return state == ServiceState.STARTED;
  }

  /**
   * Service state.
   */
  public enum ServiceState {
    WILL_START, STARTING, START_FAILED, STARTED, WILL_STOP, STOPPING, STOPPED
  }
}
