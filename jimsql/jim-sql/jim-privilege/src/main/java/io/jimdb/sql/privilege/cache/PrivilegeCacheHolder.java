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
package io.jimdb.sql.privilege.cache;

import java.util.Collections;

/**
 * @version V1.0
 */
public final class PrivilegeCacheHolder {
  private static volatile PrivilegeCache privilegeCache = new PrivilegeCache(Collections.EMPTY_LIST,
          Collections.EMPTY_LIST, Collections.EMPTY_LIST, 0);

  private PrivilegeCacheHolder() {
  }

  public static synchronized void set(PrivilegeCache update) {
    privilegeCache = update;
  }

  public static PrivilegeCache get() {
    return privilegeCache;
  }
}
