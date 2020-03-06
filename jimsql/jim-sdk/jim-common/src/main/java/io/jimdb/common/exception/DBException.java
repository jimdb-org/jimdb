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
package io.jimdb.common.exception;

/**
 * General exception related to accessing data from DS
 */
public final class DBException extends BaseException {
  private DBException(ErrorModule module, ErrorCode code, String message, Throwable cause) {
    super(module, code, message, cause);
  }

  public static DBException get(ErrorModule module, ErrorCode code, String... params) {
    return get(module, code, true, params);
  }

  public static DBException get(ErrorModule module, ErrorCode code, boolean format, String... params) {
    return new DBException(module, code, format ? message(code, params) : params[0], null);
  }

  public static DBException get(ErrorModule module, ErrorCode code, Throwable cause, String... params) {
    return new DBException(module, code, message(code, params), cause);
  }
}
