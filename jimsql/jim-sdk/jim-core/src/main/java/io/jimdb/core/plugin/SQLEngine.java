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

import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.variable.SysVariable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * Protocol codec interface.
 *
 * @version V1.0
 */
public interface SQLEngine extends Plugin {
  /**
   * @param executor
   */
  void setSQLExecutor(SQLExecutor executor);

  /**
   * Return the sql type.
   *
   * @return
   */
  DBType getType();

  /**
   * Get server status flag.
   *
   * @param status
   * @return
   */
  short getServerStatusFlag(ServerStatus status);

  /**
   * Return system Variable.
   *
   * @param name
   * @return
   */
  SysVariable getSysVariable(String name);

  /**
   * Return all the global system variable values.
   *
   * @return
   */
  Map<String, SysVariable> getAllVars();

  /**
   * Protocol frame decode.
   *
   * @param in
   */
  ByteBuf frameDecode(Session session, ByteBuf in);

  /**
   * The client and server negotiate compatibility features and authenticate when the
   * connection is initialized. After handshake, client can send request to server.
   *
   * @param session the connection context
   */
  void handShake(Session session);

  /**
   * handles client request.
   *
   * @param session
   * @param in
   */
  void handleCommand(Session session, ByteBuf in);

  /**
   * @param session
   * @param out
   * @param execResult
   * @throws BaseException
   */
  void writeResult(Session session, CompositeByteBuf out, ExecResult execResult);

  /**
   * @param session
   * @param out
   * @param exception
   * @throws BaseException
   */
  void writeError(Session session, CompositeByteBuf out, BaseException exception);

  /**
   * Sql engine type.
   */
  enum DBType {
    MYSQL("mysql"), POSTGRESQL("postgresql"), SQL_SERVER("sqlserver"), DB2("db2");
    private final String name;

    DBType(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    @Override
    public final String toString() {
      return "The sql type is: " + this.name;
    }
  }

  /**
   * Server status.
   */
  enum ServerStatus {
    INTRANS, AUTOCOMMIT, MORERESULTSEXISTS, NOGOODINDEXUSED, NOINDEXUSED, CURSOREXISTS,
    LASTROWSEND, DBDROPPED, NOBACKSLASHESCAPED, METACHANGED, WASSLOW, OUTPARAMS;
  }
}
