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
package io.jimdb.sql.optimizer;

import java.util.List;

/**
 * @version V1.0
 */
public final class IndexHint {

  private List<String> indexNames;
  private HintType hintType;
  private HintScope hintScope;

  public IndexHint(List<String> indexNames, HintType hintType, HintScope hintScope) {
    this.indexNames = indexNames;
    this.hintType = hintType;
    this.hintScope = hintScope;
  }

  public List<String> getIndexNames() {
    return indexNames;
  }

  public void setIndexNames(List<String> indexNames) {
    this.indexNames = indexNames;
  }

  public HintType getHintType() {
    return hintType;
  }

  public IndexHint setHintType(HintType hintType) {
    this.hintType = hintType;
    return this;
  }

  public HintScope getHintScope() {
    return hintScope;
  }

  public IndexHint setHintScope(HintScope hintScope) {
    this.hintScope = hintScope;
    return this;
  }

  /**
   * Use, ignore, force
   */
  public enum HintType {
    USE_INDEX_HINT,
    IGNORE_INDEX_HINT,
    FORCE_INDEX_HINT
  }

  /**
   * scan, join, orderBy, groupBy
   */
  public enum HintScope {
    SCAN_INDEX_HINT,
    JOIN_INDEX_HINT,
    ORDER_BY_INDEX_HINT,
    GROUP_BY_INDEX_HINT
  }

}
