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
package io.jimdb.core.plugin;

import io.jimdb.core.Session;

/**
 * Processor for the command.
 * The implementation of this interface must be guaranteed to be thread-safe.
 *
 * @version V1.0
 * @ThreadSafe
 */
public interface SQLExecutor extends Plugin {
  /**
   * Responsible for executing SQL statements and writing the result to the client.
   * This method does not allow any exceptions to be thrown.
   *
   * @param s
   * @param sql
   */
  void executeQuery(Session s, String sql);

  void executePrepare(Session s, String sql);

  void execute(Session s, int stmtId);
}
