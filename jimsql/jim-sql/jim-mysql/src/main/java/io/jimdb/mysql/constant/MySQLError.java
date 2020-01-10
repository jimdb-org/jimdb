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
package io.jimdb.mysql.constant;

import java.util.EnumMap;

import io.jimdb.common.exception.ErrorCode;

/**
 * Responsible for converting error encode from jimdb to mysql database.
 *
 * @version V1.0
 */
public final class MySQLError {
  private static final MySQLErrorCode DEFAULT_CODE = MySQLErrorCode.ER_UNKNOWN_ERROR;
  private static final EnumMap<ErrorCode, MySQLErrorCode> CODE_MAPS = new EnumMap<>(ErrorCode.class);

  static {
    for (ErrorCode code : ErrorCode.values()) {
      for (MySQLErrorCode mysqlCode : MySQLErrorCode.values()) {
        if (code.getCode() == mysqlCode.getCode()) {
          CODE_MAPS.put(code, mysqlCode);
          break;
        }
      }
    }
  }

  public static MySQLErrorCode toMySQLErrorCode(ErrorCode code) {
    MySQLErrorCode mySQLErrorCode = CODE_MAPS.get(code);
    if (mySQLErrorCode == null) {
      return DEFAULT_CODE;
    }
    return mySQLErrorCode;
  }
}
