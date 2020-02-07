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

import io.jimdb.common.utils.os.SystemClock;

/**
 * @version V1.0
 */
public final class PreparedPlanner {
  private final long metaVersion;
  private final long createTime;
  private final String catalog;
  private final Object planner;

  public PreparedPlanner(long metaVersion, String catalog, Object planner) {
    this.metaVersion = metaVersion;
    this.catalog = catalog;
    this.planner = planner;
    this.createTime = SystemClock.currentTimeMillis();
  }

  public long getMetaVersion() {
    return metaVersion;
  }

  public String getCatalog() {
    return catalog;
  }

  public Object getPlanner() {
    return planner;
  }

  public boolean isTimeout(long ttl) {
    return (SystemClock.currentTimeMillis() - createTime) > ttl;
  }
}
