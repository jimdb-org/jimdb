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

import io.jimdb.common.utils.lang.ByteUtil;

/**
 * @version V1.0
 */
public class RangeRouteException extends JimException {

  public byte[] key;

  private RangeRouteException(ErrorModule module, ErrorCode code, String message, Throwable cause,
                              byte[] key) {
    super(module, code, message, cause);
    this.key = key;
  }

  public static RangeRouteException get(ErrorModule module, ErrorCode code, byte[] key) {
    return new RangeRouteException(module, code, message(code, ByteUtil.bytes2hex01(key)), null, key);
  }

  public static RangeRouteException get(ErrorModule module, ErrorCode code, Throwable cause, byte[] key) {
    return new RangeRouteException(module, code, message(code, ByteUtil.bytes2hex01(key)), cause, key);
  }
}
