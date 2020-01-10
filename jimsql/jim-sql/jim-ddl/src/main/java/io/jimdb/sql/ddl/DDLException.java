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
package io.jimdb.sql.ddl;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;

/**
 * @version V1.0
 */
final class DDLException extends JimException {
  private static final long serialVersionUID = -2943328982200719266L;

  final ErrorType type;

  DDLException(ErrorType type, String message) {
    super(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, message, null);
    this.type = type;
  }

  DDLException(ErrorType type, Throwable cause) {
    super(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, cause.getMessage(), cause);
    this.type = type;
  }

  DDLException(ErrorType type, ErrorCode code, Throwable cause) {
    super(ErrorModule.DDL, code, cause.getMessage(), cause);
    this.type = type;
  }

  DDLException(ErrorType type, ErrorCode code, String... params) {
    this(type, code, true, params);
  }

  DDLException(ErrorType type, ErrorCode code, boolean format, String... params) {
    super(ErrorModule.DDL, code, format ? message(code, params) : params[0], null);
    this.type = type;
  }

  /**
   *
   */
  enum ErrorType {
    FAILED, ROLLACK, CONCURRENT
  }
}
