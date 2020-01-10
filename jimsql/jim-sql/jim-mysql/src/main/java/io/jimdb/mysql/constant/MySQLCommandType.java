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
public enum MySQLCommandType {

  MYSQL_COM_SLEEP(0x00),

  MYSQL_COM_QUIT(0x01),

  MYSQL_COM_INIT_DB(0x02),

  MYSQL_COM_QUERY(0x03),

  MYSQL_COM_FIELD_LIST(0x04),

  MYSQL_COM_CREATE_DB(0x05),

  MYSQL_COM_DROP_DB(0x06),

  MYSQL_COM_REFRESH(0x07),

  MYSQL_COM_SHUTDOWN(0x08),

  MYSQL_COM_STATISTICS(0x09),

  MYSQL_COM_PROCESS_INFO(0x0a),

  MYSQL_COM_CONNECT(0x0b),

  MYSQL_COM_PROCESS_KILL(0x0c),

  MYSQL_COM_DEBUG(0x0d),

  MYSQL_COM_PING(0x0e),

  MYSQL_COM_TIME(0x0f),

  MYSQL_COM_DELAYED_INSERT(0x10),

  MYSQL_COM_CHANGE_USER(0x11),

  MYSQL_COM_BINLOG_DUMP(0x12),

  MYSQL_COM_TABLE_DUMP(0x13),

  MYSQL_COM_CONNECT_OUT(0x14),

  MYSQL_COM_REGISTER_SLAVE(0x15),

  MYSQL_COM_STMT_PREPARE(0x16),

  MYSQL_COM_STMT_EXECUTE(0x17),

  MYSQL_COM_STMT_SEND_LONG_DATA(0x18),

  MYSQL_COM_STMT_CLOSE(0x19),

  MYSQL_COM_STMT_RESET(0x1a),

  MYSQL_COM_SET_OPTION(0x1b),

  MYSQL_COM_STMT_FETCH(0x1c),

  MYSQL_COM_DAEMON(0x1d),

  MYSQL_COM_BINLOG_DUMP_GTID(0x1e),

  MYSQL_COM_RESET_CONNECTION(0x1f);

  private final int value;

  public int getValue() {
    return value;
  }

  MySQLCommandType(int value) {
    this.value = value;
  }

  public static MySQLCommandType valueOf(final int value) {
    for (MySQLCommandType each : MySQLCommandType.values()) {
      if (value == each.value) {
        return each;
      }
    }
    throw new IllegalArgumentException(String.format("Cannot find '%s' in command packet type", value));
  }
}
