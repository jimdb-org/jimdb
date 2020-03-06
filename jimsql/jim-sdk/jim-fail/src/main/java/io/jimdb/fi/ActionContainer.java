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
package io.jimdb.fi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An ActionContainer contains at most one action.
 * The definition format is: [<pct>%][<cnt>*]<type>[(args...)][-><more terms>]
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "MDM_THREAD_YIELD", "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", "WA_AWAIT_NOT_IN_LOOP" })
final class ActionContainer {
  private final ReentrantReadWriteLock rwLock;
  private final Condition pause;

  private long version;
  private String actionStr;
  private Action[] actions;

  ActionContainer() {
    this.rwLock = new ReentrantReadWriteLock();
    this.pause = this.rwLock.writeLock().newCondition();
    this.actionStr = "";
    this.actions = null;
    this.version = 0;
  }

  static Action[] parseActions(String s) {
    if (StringUtils.isBlank(s)) {
      return null;
    }

    String[] splits = s.split("->");
    List<Action> actions = new ArrayList<>(splits.length);
    for (String str : splits) {
      actions.add(Action.parseAction(str));
    }
    return actions.toArray(new Action[0]);
  }

  void setActions(String s) {
    rwLock.writeLock().lock();

    try {
      this.actionStr = s == null ? "" : s;
      this.actions = parseActions(s);
      this.version += 1;

      pause.signalAll();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  String execute(String name) {
    rwLock.readLock().lock();

    Action action = null;
    long curVersion = this.version;
    try {
      if (actions == null || actions.length == 0) {
        return null;
      }

      for (Action a : actions) {
        if (a.injectCriteria()) {
          action = a;
          break;
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }

    if (action == null) {
      return null;
    }

    switch (action.getType()) {
      case OFF:
        break;
      case RETURN:
        return action.getArg();
      case PRINT:
        if (StringUtils.isBlank(action.getArg())) {
          System.out.println(String.format("failpoint %s executed", name));
        } else {
          System.out.println(action.getArg());
        }
        break;
      case YIELD:
        Thread.yield();
        break;
      case SLEEP:
      case DELAY:
        try {
          Thread.sleep(Long.parseLong(action.getArg().trim()));
        } catch (Exception ex) {
        }
        break;
      case PAUSE:
        rwLock.writeLock().lock();
        try {
          if (curVersion == this.version) {
            if (StringUtils.isBlank(action.getArg())) {
              pause.await();
            } else {
              pause.await(Long.parseLong(action.getArg().trim()), TimeUnit.MILLISECONDS);
            }
          }
        } catch (Exception ex) {
        } finally {
          rwLock.writeLock().unlock();
        }
        break;
      case PANIC:
        if (StringUtils.isBlank(action.getArg())) {
          throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
                  String.format("failpoint %s panic", name));
        }

        String[] params = action.getArg().split(";");
        if (params.length == 1) {
          throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false, params[0]);
        }
        throw DBException.get(ErrorModule.SYSTEM, ErrorCode.valueOf(params[0]), ArrayUtils.subarray(params, 1, params.length));
      default:
        break;
    }

    return null;
  }

  @Override
  public String toString() {
    rwLock.readLock().lock();
    try {
      return actionStr;
    } finally {
      rwLock.readLock().unlock();
    }
  }
}
