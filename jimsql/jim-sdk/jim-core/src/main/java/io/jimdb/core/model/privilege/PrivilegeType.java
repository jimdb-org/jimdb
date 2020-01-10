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
package io.jimdb.core.model.privilege;

import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 */
public enum PrivilegeType {
  CREATE_PRIV(1 << 1),
  SELECT_PRIV(1 << 2),
  INSERT_PRIV(1 << 3),
  UPDATE_PRIV(1 << 4),
  DELETE_PRIV(1 << 5),
  SHOW_DB_PRIV(1 << 6),
  SUPER_PRIV(1 << 7),
  CREATE_USER_PRIV(1 << 8),
  TRIGGER_PRIV(1 << 9),
  DROP_PRIV(1 << 10),
  PROCESS_PRIV(1 << 11),
  GRANT_PRIV(1 << 12),
  REFERENCES_PRIV(1 << 13),
  ALTER_PRIV(1 << 14),
  EXECUTE_PRIV(1 << 15),
  INDEX_PRIV(1 << 16),
  CREATE_VIEW_PRIV(1 << 17),
  SHOW_VIEW_PRIV(1 << 18),
  CREATE_ROLE_PRIV(1 << 19),
  DROP_ROLE_PRIV(1 << 20),
  ALLPRIV(1 << 21);

  PrivilegeType(int code) {
    this.code = code;
  }

  private final int code;

  public int getCode() {
    return code;
  }

  public static PrivilegeType getType(String name) {
    return PrivilegeType.valueOf(name.toUpperCase());
  }

  public static String privilegesToString(int privs) {
    List<String> stringList = new ArrayList<>(22);
    for (PrivilegeType privilegeType : PrivilegeType.values()) {
      if ((privs & privilegeType.getCode()) > 0) {
        stringList.add(privilegeType.toString());
      }
    }

    return String.join(",", stringList);
  }
}
