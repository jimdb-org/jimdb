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
package io.jimdb.core.context;

/**
 * @version V1.0
 */
public final class ReorgContext {
  public static final int REORG_CONCURRENT_NUM = 2;
  public static final int REORG_TXN_LIMIT_NUM = 10000;

//  private byte[] lastRowKey;
//  private byte[] minRangeKey;

  private volatile boolean failed = false;

  private Throwable err;

  public void setFailed(boolean failed, Throwable e) {
    this.failed = failed;
    this.err = e;
  }

  public void setFailed(boolean failed) {
    this.failed = failed;
  }

  public boolean isFailed() {
    return failed;
  }

  public Throwable getErr() {
    return err;
  }

  public void setErr(Throwable err) {
    this.err = err;
  }
}
