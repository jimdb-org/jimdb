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
public enum CapabilityFlags {

  MYSQL_CLIENT_LONG_PASSWORD(0x00000001),

  MYSQL_CLIENT_FOUND_ROWS(0x00000002),

  MYSQL_CLIENT_LONG_FLAG(0x00000004),

  MYSQL_CLIENT_CONNECT_WITH_DB(0x00000008),

  MYSQL_CLIENT_NO_SCHEMA(0x00000010),

  MYSQL_CLIENT_COMPRESS(0x00000020),

  MYSQL_CLIENT_ODBC(0x00000040),

  MYSQL_CLIENT_LOCAL_FILES(0x00000080),

  MYSQL_CLIENT_IGNORE_SPACE(0x00000100),

  MYSQL_CLIENT_PROTOCOL_41(0x00000200),

  MYSQL_CLIENT_INTERACTIVE(0x00000400),

  MYSQL_CLIENT_SSL(0x00000800),

  MYSQL_CLIENT_IGNORE_SIGPIPE(0x00001000),

  MYSQL_CLIENT_TRANSACTIONS(0x00002000),

  MYSQL_CLIENT_RESERVED(0x00004000),

  MYSQL_CLIENT_SECURE_CONNECTION(0x00008000),

  MYSQL_CLIENT_MULTI_STATEMENTS(0x00010000),

  MYSQL_CLIENT_MULTI_RESULTS(0x00020000),

  MYSQL_CLIENT_PS_MULTI_RESULTS(0x00040000),

  MYSQL_CLIENT_PLUGIN_AUTH(0x00080000),

  MYSQL_CLIENT_CONNECT_ATTRS(0x00100000),

  MYSQL_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000),

  MYSQL_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000),

  MYSQL_CLIENT_SESSION_TRACK(0x00800000),

  MYSQL_CLIENT_DEPRECATE_EOF(0x01000000);

  private final int value;

  CapabilityFlags(int value) {
    this.value = value;
  }

  public static int getDefaultCapabilityFlagsLower() {
    return includeCapabilityFlags(MYSQL_CLIENT_LONG_PASSWORD, MYSQL_CLIENT_LONG_FLAG, MYSQL_CLIENT_CONNECT_WITH_DB,
            MYSQL_CLIENT_PROTOCOL_41, MYSQL_CLIENT_TRANSACTIONS, MYSQL_CLIENT_SECURE_CONNECTION);
  }

  public static int getDefaultCapabilityFlagsUpper() {
    return 0;
  }

  private static int includeCapabilityFlags(final CapabilityFlags... capabilities) {
    int result = 0;
    for (CapabilityFlags each : capabilities) {
      result |= each.value;
    }
    return result;
  }

  public int getValue() {
    return value;
  }
}
