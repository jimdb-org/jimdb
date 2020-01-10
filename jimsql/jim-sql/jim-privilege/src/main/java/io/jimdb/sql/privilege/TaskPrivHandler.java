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
package io.jimdb.sql.privilege;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Ddlpb.PriOn;
import io.jimdb.pb.Ddlpb.PriUser;
import io.jimdb.pb.Ddlpb.PrivilegeOp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("OCP_OVERLY_CONCRETE_PARAMETER")
final class TaskPrivHandler {

  private TaskPrivHandler() {
  }

  static boolean grantPriv(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>();

    PriUser user = op.getUsers(0);
    if (PrivilegeStore.userNotExists(user)) {
      sqls.add(PrivilegeStore.insertUser(user));
    }

    PriOn on = op.getOn();
    switch (on.getLevel()) {
      case DB:
        if (PrivilegeStore.dbNotExists(user, on)) {
          sqls.add(PrivilegeStore.insertDb(user, on));
        }
        break;
      case Table:
        if (PrivilegeStore.tableNotExists(user, on)) {
          sqls.add(PrivilegeStore.insertTable(user, on));
        }
        break;
      default:
        break;
    }

    List<String> prives = op.getPrivilegesList();
    switch (on.getLevel()) {
      case Global:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.updateUser(user, null, "Y"));
        } else {
          sqls.add(PrivilegeStore.updateUser(user, prives, "Y"));
        }
        break;
      case DB:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.updateDb(user, on, null, "Y"));
        } else {
          sqls.add(PrivilegeStore.updateDb(user, on, prives, "Y"));
        }
        break;
      case Table:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.updateTable(user, on, null));
        } else {
          sqls.add(PrivilegeStore.updateTable(user, on, prives));
        }
        break;
      default:
        break;
    }

    // Execute in a transaction
    if (sqls.size() > 0) {
      ExecResult result = PrivilegeStore.execute(sqls.toArray(new String[0]));
      return result.getAffectedRows() != 0;
    }
    return true;
  }

  static boolean revokePriv(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>();

    PriUser user = op.getUsers(0);
    if (PrivilegeStore.userNotExists(user)) {
      throw new PrivilegeException(ErrorCode.ER_NONEXISTING_GRANT, user.getName(), user.getHost());
    }

    PriOn on = op.getOn();
    switch (on.getLevel()) {
      case DB:
        if (PrivilegeStore.dbNotExists(user, on)) {
          throw new PrivilegeException(ErrorCode.ER_NONEXISTING_GRANT, user.getName(), user.getHost());
        }
        break;
      case Table:
        if (PrivilegeStore.tableNotExists(user, on)) {
          throw new PrivilegeException(ErrorCode.ER_NONEXISTING_TABLE_GRANT, user.getName(), user.getHost(), on.getTable());
        }
        break;
      default:
        break;
    }

    List<String> prives = op.getPrivilegesList();
    switch (on.getLevel()) {
      case Global:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.updateUser(user, null, "N"));
        } else {
          sqls.add(PrivilegeStore.updateUser(user, prives, "N"));
        }
        break;
      case DB:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.updateDb(user, on, null, "N"));
        } else {
          sqls.add(PrivilegeStore.updateDb(user, on, prives, "N"));
        }
        break;
      case Table:
        if (prives.size() == 1 && "ALL".equals(prives.get(0)) || "ALL PRIVILEGES".equals(prives.get(0))) {
          sqls.add(PrivilegeStore.revokeTable(user, on, Collections.EMPTY_LIST));
        } else {
          sqls.add(PrivilegeStore.revokeTable(user, on, prives));
        }
        break;
      default:
        break;

    }

    // Execute in a transaction
    if (sqls.size() > 0) {
      ExecResult result = PrivilegeStore.execute(sqls.toArray(new String[0]));
      return result.getAffectedRows() != 0;
    }
    return true;
  }

  static boolean setPassword(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>(op.getUsersCount());
    for (PriUser user : op.getUsersList()) {
      if (PrivilegeStore.userNotExists(user)) {
        throw new PrivilegeException(ErrorCode.ER_PASSWORD_NO_MATCH);
      }
      sqls.add(PrivilegeStore.updatePassword(user));
    }

    if (sqls.size() > 0) {
      ExecResult result = PrivilegeStore.execute(sqls.toArray(new String[0]));
      return result.getAffectedRows() != 0;
    }
    return true;
  }

  static boolean createUser(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>();
    for (PriUser user : op.getUsersList()) {
      if (PrivilegeStore.userNotExists(user)) {
        sqls.add(PrivilegeStore.insertUser(user));
      }
    }

    if (sqls.size() > 0) {
      ExecResult result = PrivilegeStore.execute(sqls.toArray(new String[0]));
      return result.getAffectedRows() != 0;
    }
    return true;
  }

  static boolean alterUser(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>();
    for (PriUser user : op.getUsersList()) {
      if (PrivilegeStore.userNotExists(user)) {
        sqls.add(PrivilegeStore.insertUser(user));
      } else {
        sqls.add(PrivilegeStore.updatePassword(user));
      }
    }

    if (sqls.size() > 0) {
      ExecResult result = PrivilegeStore.execute(sqls.toArray(new String[0]));
      return result.getAffectedRows() != 0;
    }
    return true;
  }

  static boolean dropUser(PrivilegeOp op) {
    List<String> sqls = new ArrayList<>(op.getPrivilegesCount() * 3);
    for (PriUser user : op.getUsersList()) {
      sqls.add(PrivilegeStore.deleteUser(user));
      sqls.add(PrivilegeStore.deleteCatalog(user));
      sqls.add(PrivilegeStore.deleteTable(user));
    }

    if (sqls.size() > 0) {
      PrivilegeStore.execute(sqls.toArray(new String[0]));
    }
    return true;
  }
}
