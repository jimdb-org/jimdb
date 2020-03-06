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
package io.jimdb.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.SQLExecutor;
import io.jimdb.core.plugin.store.Engine;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class ExecutorHelper {
  private ExecutorHelper() {
  }

  public static List<ExecResult> execute(SQLExecutor sqlExecutor, SQLEngine sqlEngine, Engine storeEngine, String... sqls) {
    if (sqls == null || sqls.length == 0) {
      return Collections.EMPTY_LIST;
    }

    final SyncSession session = new SyncSession(sqlEngine, storeEngine, sqls.length);
    sqlExecutor.executeQuery(session, StringUtils.join(sqls, ";"));
    return session.get();
  }

  /**
   *
   */
  public static final class SyncSession extends Session {
    private List<ExecResult> results;
    private BaseException error;
    private final CountDownLatch latch;

    SyncSession(SQLEngine sqlEngine, Engine storeEngine, int num) {
      super(sqlEngine, storeEngine);
      this.latch = new CountDownLatch(num);
      this.results = new ArrayList<>(num);
    }

    @Override
    public void write(ExecResult rs, boolean isEof) {
      results.add(rs);
      latch.countDown();
    }

    public List<ExecResult> get() {
      try {
        latch.await();
        if (error == null) {
          return results;
        }

        throw error;
      } catch (InterruptedException ex) {
        throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_INTERNAL_ERROR, ex);
      } finally {
        this.close();
      }
    }

    @Override
    public void writeError(BaseException ex) {
      error = ex;
      final long size = latch.getCount();
      for (long i = 0; i < size; i++) {
        latch.countDown();
      }
    }
  }
}
