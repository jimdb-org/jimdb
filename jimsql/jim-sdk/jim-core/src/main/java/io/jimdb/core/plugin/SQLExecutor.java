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
package io.jimdb.core.plugin;

import javax.annotation.concurrent.ThreadSafe;

import io.jimdb.core.Session;

/**
 * Processor for the command.
 * The implementation of this interface must be guaranteed to be thread-safe.
 */
@ThreadSafe
public interface SQLExecutor extends Plugin {
  /**
   * Process MySql MYSQL_COM_QUERY command - executing a SQL statement and writing the result to the client.
   * This method does not allow any exceptions to be thrown.
   *
   * @param s session
   * @param sql sql statement to be executed
   */
  void executeQuery(Session s, String sql);


  /**
   * Process MySql COM_STMT_PREPARE command
   * @param s session
   * @param sql sql statement to be prepared
   */
  void createPrepare(Session s, String sql);

  /**
   * Process MySql MYSQL_COM_STMT_EXECUTE command
   * @param s session
   * @param stmtId id of the statement to be executed
   */
  void executePrepare(Session s, int stmtId);
}
