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

import java.util.EnumMap;
import java.util.Locale;
import java.util.Properties;

import io.jimdb.common.utils.lang.IOUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public abstract class JimException extends RuntimeException {
  protected static final long serialVersionUID = 2767283925928234491L;

  @SuppressFBWarnings("PMB_POSSIBLE_MEMORY_BLOAT")
  private static final EnumMap<ErrorCode, String> CODE_MESSAGES = new EnumMap(ErrorCode.class);

  protected final ErrorModule module;
  protected final ErrorCode code;

  static {
    final String lan = Locale.getDefault().getLanguage();
    final String resource = "code_messages_" + lan + ".properties";
    try {
      final Properties props = IOUtil.loadResource(resource);
      props.forEach((code, msg) -> {
        ErrorCode error = ErrorCode.valueOf(String.valueOf(code));
        if (error != null) {
          CODE_MESSAGES.put(error, msg.toString());
        }
      });
    } catch (Exception ex) {
    }
  }

  public JimException(ErrorModule module, ErrorCode code, String message, Throwable cause) {
    super(message, cause);
    this.module = module;
    this.code = code;
  }

  public static final String message(ErrorCode code, String... params) {
    String msg = (CODE_MESSAGES.isEmpty() || !CODE_MESSAGES.containsKey(code)) ? code.getMessage() : CODE_MESSAGES.get(code);
    if (params != null) {
      msg = String.format(msg, params);
    }
    return msg;
  }

  public final ErrorCode getCode() {
    return this.code;
  }

  public final ErrorModule getModule() {
    return this.module;
  }

  @Override
  public final String toString() {
    StringBuilder buf = new StringBuilder(128);
    buf.append('[')
            .append(this.module)
            .append('-')
            .append(this.code)
            .append(']')
            .append(this.getMessage());
    return buf.toString();
  }
}
