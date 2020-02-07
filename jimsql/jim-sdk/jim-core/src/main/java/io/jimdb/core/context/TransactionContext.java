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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.jimdb.common.utils.lang.Resetable;
import io.jimdb.core.model.meta.MetaData;

/**
 * @version V1.0
 */
public final class TransactionContext implements Resetable {
  private static final AtomicIntegerFieldUpdater<TransactionContext> REFCNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(TransactionContext.class, "refCnt");

  private volatile int refCnt;
  private volatile MetaData metaData;

  public MetaData getMetaData() {
    MetaData result = metaData;
    if (result == null) {
      result = MetaData.Holder.get();
      metaData = result;
    }
    return result;
  }

  public void init() {
    if (REFCNT_UPDATER.compareAndSet(this, 0, 1)) {
      this.metaData = MetaData.Holder.getRetained();
    }
  }

  public void metaRelease() {
    if (REFCNT_UPDATER.compareAndSet(this, 1, 0)) {
      MetaData.Holder.release(this.metaData);
    }
  }

  @Override
  public void reset() {
    this.metaRelease();
    this.metaData = null;
  }

  @Override
  public void close() {
    this.reset();
  }
}
