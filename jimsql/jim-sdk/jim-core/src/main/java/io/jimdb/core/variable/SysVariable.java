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
package io.jimdb.core.variable;

/**
 * @version V1.0
 */
public final class SysVariable {
  public static final int SCOPE_NONE = 0;
  public static final int SCOPE_GLOBAL = 1 << 0;
  public static final int SCOPE_SESSION = 1 << 1;

  private final String name;
  private final String value;
  private final int scope;

  public SysVariable(final int scope, final String name, final String value) {
    this.name = name;
    this.value = value;
    this.scope = scope;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public int getScope() {
    return scope;
  }

  public boolean isNone() {
    return this.scope == SCOPE_NONE;
  }

  public boolean isGlobal() {
    return (this.scope & SCOPE_GLOBAL) != 0;
  }

  public boolean isSession() {
    return (this.scope & SCOPE_SESSION) != 0;
  }
}
