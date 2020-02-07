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
package io.jimdb.mysql.constant;

/**
 * @version V1.0
 */
public enum MySQLColumnFlag {

  NOT_NULL_FLAG(1),

  PRI_KEY_FLAG(2),

  UNIQUE_KEY_FLAG(4),

  MULTIPLE_KEY_FLAG(8),

  BLOB_FLAG(16),

  UNSIGNED_FLAG(32),

  ZEROFILL_FLAG(64),

  BINARY_FLAG(128),

  ENUM_FLAG(256),

  AUTO_INCREMENT_FLAG(512);

  private final int value;

  public int getValue() {
    return value;
  }

  MySQLColumnFlag(int value) {
    this.value = value;
  }
}
