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
package io.jimdb.sql.privilege;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.ExecutorHelper;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Ddlpb.PriOn;
import io.jimdb.pb.Ddlpb.PriUser;
import io.jimdb.core.plugin.PluginFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("OCP_OVERLY_CONCRETE_PARAMETER")
public final class PrivilegeStore {
  private static final List<String> PRIVILEGE_USERS = Arrays.asList("Create_priv", "Select_priv", "Insert_priv", "Update_priv",
          "Delete_priv", "Show_db_priv", "Super_priv", "Create_user_priv", "Trigger_priv", "Drop_priv", "Process_priv", "Grant_priv",
          "References_priv", "Alter_priv", "Execute_priv", "Index_priv", "Create_view_priv", "Show_view_priv", "Create_role_priv", "Drop_role_priv");

  private static final List<String> PRIVILEGE_CATALOGS = Arrays.asList("Select_priv", "Insert_priv", "Update_priv", "Delete_priv",
          "Create_priv", "Drop_priv", "Grant_priv", "Alter_priv", "Index_priv", "Execute_priv", "Create_view_priv", "Show_view_priv");

  private static final List<String> PRIVILEGE_TABLES = Arrays.asList("Select_priv", "Insert_priv", "Update_priv", "Delete_priv",
          "Create_priv", "Drop_priv", "Grant_priv", "Alter_priv", "Index_priv");

//  private static final List<String> PRIVILEGE_COLUMNS = Arrays.asList("Select_priv", "Insert_priv", "Update_priv");

  private static final String SQL_LOAD_USER = "select user,password,host,Create_priv,Select_priv,Insert_priv,Update_priv,"
          + "Delete_priv,Show_db_priv,Super_priv,Create_user_priv,Trigger_priv,Drop_priv,Process_priv,Grant_priv,References_priv,"
          + "Alter_priv,Execute_priv,Index_priv,Create_view_priv,Show_view_priv,Create_role_priv,Drop_role_priv from mysql.user";

  private static final String SQL_LOAD_CATALOG = "select Host,DB,User,Select_priv,Insert_priv,Update_priv,Delete_priv,"
          + "Create_priv,Drop_priv,Grant_priv,Index_priv,Alter_priv,Execute_priv,Create_view_priv,Show_view_priv from mysql.db";

  private static final String SQL_LOAD_TABLE = "select Host,DB,User,Table_name,Table_priv,Column_priv from mysql.tables_priv";

  private static final String SQL_EXISTS_USER = "select Host, User from mysql.user where Host = '%s' and User = '%s'";

  private static final String SQL_EXISTS_CATALOG = "select Host, Db, User from mysql.db where Host = '%s' and Db = '%s' and User = '%s'";

  private static final String SQL_EXISTS_TABLE = "select Host, Db, User, Table_name from mysql.tables_priv where Host = '%s' and Db = '%s' "
          + "and User = '%s' and Table_name = '%s'";

  private static final String SQL_INSERT_USER = "INSERT INTO mysql.user(Host, User, Password) VALUES ('%s', '%s', '%s')";

  private static final String SQL_INSERT_CATALOG = "INSERT INTO mysql.db(Host, Db, User) VALUES ('%s', '%s', '%s')";

  private static final String SQL_INSERT_TABLE = "INSERT INTO mysql.tables_priv(Host, Db, User, Table_name) VALUES ('%s', '%s', '%s', '%s')";

  private static final String SQL_UPDATE_USER = "UPDATE mysql.user SET %s where Host = '%s' and User = '%s'";

  private static final String SQL_UPDATE_PASSWORD = "UPDATE mysql.user SET Password='%s' WHERE Host = '%s' and User = '%s'";

  private static final String SQL_UPDATE_CATALOG = "UPDATE mysql.db SET %s where Host = '%s' and Db = '%s' and User = '%s'";

  private static final String SQL_UPDATE_TABLE = "UPDATE mysql.tables_priv SET %s where Host = '%s' and Db = '%s' and User = '%s' and Table_name = '%s'";

  private static final String SQL_DELETE_USER = "DELETE FROM mysql.user WHERE Host = '%s' and User = '%s'";

  private static final String SQL_DELETE_CATALOG = "DELETE FROM mysql.db WHERE Host = '%s' and User = '%s'";

  private static final String SQL_DELETE_TABLE = "DELETE FROM mysql.tables_priv WHERE Host = '%s' and User = '%s'";

  private static final String SQL_INIT_USER = "INSERT INTO mysql.user(Host, User, Password, Select_priv, Insert_priv, "
          + "Update_priv, Delete_priv, Create_priv, Drop_priv, Process_priv, Grant_priv, References_priv, Alter_priv, "
          + "Show_db_priv, Super_priv, Create_tmp_table_priv, Lock_tables_priv, Execute_priv, Create_view_priv, "
          + "Show_view_priv, Create_routine_priv, Alter_routine_priv, Index_priv, Create_user_priv, Event_priv, "
          + "Trigger_priv, Create_role_priv, Drop_role_priv, Account_locked) VALUES('%', 'root', '%s', 'Y', "
          + "'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y','Y', 'Y', 'Y', 'Y'), "
          + "('127.0.0.1', 'root', '%s', 'Y', 'Y','Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', "
          + "'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y')";

  private static final String SQL_CREATE_MYSQL = "CREATE DATABASE IF NOT EXISTS mysql";

  private static final String SQL_CREATE_USER = "CREATE TABLE IF NOT EXISTS mysql.user(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
          + "Host varchar(64), User varchar(16), Password varchar(41), "
          + "Select_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Insert_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Update_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Delete_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Drop_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Process_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Grant_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "References_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Alter_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Show_db_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Super_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_tmp_table_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Lock_tables_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Execute_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_view_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Show_view_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_routine_priv varchar(4) NOT NULL DEFAULT 'N',  "
          + "Alter_routine_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Index_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_user_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Event_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Trigger_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_role_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Drop_role_priv varchar(4) NOT NULL DEFAULT 'N',  "
          + "Account_locked varchar(4) NOT NULL DEFAULT 'N' "
          + ", UNIQUE Key(Host, User)) COMMENT 'REPLICA=%d' ENGINE=%s";

  private static final String SQL_CREATE_CATALOG = "CREATE TABLE IF NOT EXISTS mysql.db(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
          + "Host varchar(64), DB varchar(64), User varchar(16), "
          + "Select_priv varchar(4) Not Null DEFAULT 'N', "
          + "Insert_priv varchar(4) Not Null DEFAULT 'N', "
          + "Update_priv varchar(4) Not Null DEFAULT 'N', "
          + "Delete_priv varchar(4) Not Null DEFAULT 'N', "
          + "Create_priv varchar(4) Not Null DEFAULT 'N', "
          + "Drop_priv varchar(4) Not Null DEFAULT 'N', "
          + "Grant_priv varchar(4) Not Null DEFAULT 'N', "
          + "References_priv varchar(4) Not Null DEFAULT 'N', "
          + "Index_priv varchar(4) Not Null DEFAULT 'N', "
          + "Alter_priv varchar(4) Not Null DEFAULT 'N', "
          + "Create_tmp_table_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Lock_tables_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_view_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Show_view_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Create_routine_priv varchar(4) NOT NULL DEFAULT 'N', "
          + " Alter_routine_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Execute_priv varchar(4) Not Null DEFAULT 'N', "
          + "Event_priv varchar(4) NOT NULL DEFAULT 'N', "
          + "Trigger_priv varchar(4) NOT NULL DEFAULT 'N' "
          + ", UNIQUE Key(Host, DB, User)) COMMENT 'REPLICA=%d' ENGINE=%s";

  private static final String SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS mysql.tables_priv(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
          + "Host varchar(64), DB varchar(64), User varchar(16), Table_name varchar(64), "
          + "Grantor varchar(77), "
          + "Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP, "
          + "Table_priv	varchar(255), "
          + "Column_priv	varchar(255)"
          + ", UNIQUE Key(Host, DB, User, Table_name)) COMMENT 'REPLICA=%d' ENGINE=%s";

  private PrivilegeStore() {
  }

  protected static void init(String engine, int replica) {
    if (isInited()) {
      return;
    }

    execute(SQL_CREATE_MYSQL);
    execute(String.format(SQL_CREATE_USER, replica, engine));
    execute(String.format(SQL_CREATE_CATALOG, replica, engine));
    execute(String.format(SQL_CREATE_TABLE, replica, engine));
    String root = ScrambleUtil.encodePassword("root");
    execute(String.format(SQL_INIT_USER, root, root));
  }

  protected static boolean isInited() {
    PriUser user = PriUser.newBuilder()
            .setName("root")
            .setHost("localhost")
            .build();

    try {
      return !userNotExists(user);
    } catch (DBException e) {
      // if mysql.user not exists
      if (e.getCode() != ErrorCode.ER_BAD_TABLE_ERROR && e.getCode() != ErrorCode.ER_BAD_DB_ERROR) {
        throw e;
      }
    }

    return false;
  }

  protected static ExecResult loadUser() {
    return execute(SQL_LOAD_USER);
  }

  protected static ExecResult loadDB() {
    return execute(SQL_LOAD_CATALOG);
  }

  protected static ExecResult loadTable() {
    return execute(SQL_LOAD_TABLE);
  }

  protected static boolean userNotExists(PriUser user) {
    ExecResult result = execute(String.format(SQL_EXISTS_USER, user.getHost(), user.getName()));
    return result.size() == 0;
  }

  protected static boolean dbNotExists(PriUser user, PriOn on) {
    ExecResult result = execute(String.format(SQL_EXISTS_CATALOG, user.getHost(), on.getDb(), user.getName()));
    return result.size() == 0;
  }

  protected static boolean tableNotExists(PriUser user, PriOn on) {
    ExecResult result = execute(String.format(SQL_EXISTS_TABLE, user.getHost(), on.getDb(), user.getName(), on.getTable()));
    return result.size() == 0;
  }

  protected static String insertUser(PriUser user) {
    return String.format(SQL_INSERT_USER, user.getHost(), user.getName(), ScrambleUtil.encodePassword(user.getPassword()));
  }

  protected static String insertDb(PriUser user, PriOn on) {
    return String.format(SQL_INSERT_CATALOG, user.getHost(), on.getDb(), user.getName());
  }

  protected static String insertTable(PriUser user, PriOn on) {
    return String.format(SQL_INSERT_TABLE, user.getHost(), on.getDb(), user.getName(), on.getTable());
  }

  protected static String updatePassword(PriUser user) {
    return String.format(SQL_UPDATE_PASSWORD, ScrambleUtil.encodePassword(user.getPassword()), user.getHost(), user.getName());
  }

  protected static String updateUser(PriUser user, List<String> prives, String value) {
    if (null == prives || prives.isEmpty()) {
      prives = PRIVILEGE_USERS;
    }

    return String.format(SQL_UPDATE_USER, buildPrivilegeSet(prives, value), user.getHost(), user.getName());
  }

  protected static String updateDb(PriUser user, PriOn on, List<String> prives, String value) {
    if (null == prives || prives.isEmpty()) {
      prives = PRIVILEGE_CATALOGS;
    }

    return String.format(SQL_UPDATE_CATALOG, buildPrivilegeSet(prives, value), user.getHost(), on.getDb(), user.getName());
  }

  protected static String updateTable(PriUser user, PriOn on, List<String> prives) {
    if (null == prives || prives.isEmpty()) {
      prives = PRIVILEGE_TABLES;
    }

    StringBuilder sb = new StringBuilder();
    for (String priv : prives) {
      sb.append(priv).append(',');
    }
    return String.format(SQL_UPDATE_TABLE, String.format("Table_priv='%s'", sb.length() == 0 ? "" : sb.substring(0, sb.length() - 1)),
            user.getHost(), on.getDb(), user.getName(), on.getTable());
  }

  protected static String deleteUser(PriUser user) {
    return String.format(SQL_DELETE_USER, user.getHost(), user.getName());
  }

  protected static String deleteCatalog(PriUser user) {
    return String.format(SQL_DELETE_CATALOG, user.getHost(), user.getName());
  }

  protected static String deleteTable(PriUser user) {
    return String.format(SQL_DELETE_TABLE, user.getHost(), user.getName());
  }

  protected static String revokeTable(PriUser user, PriOn on, List<String> prives) {
    if (prives.size() > 0) {
      List<String> tablePrive = getTablePrives(user, on);
      if (tablePrive != null && !tablePrive.isEmpty()) {
        for (String priv : prives) {
          tablePrive.remove(priv);
        }

        StringBuilder sb = new StringBuilder();
        for (String priv : tablePrive) {
          sb.append(priv).append(',');
        }
        return String.format(SQL_UPDATE_TABLE, String.format("Table_priv='%s'", sb.length() == 0 ? "" : sb.substring(0, sb.length() - 1)),
                user.getHost(), on.getDb(), user.getName(), on.getTable());
      }
    }

    return "";
  }

  private static List<String> getTablePrives(PriUser user, PriOn on) {
    return user == null && on == null ? null : Collections.EMPTY_LIST;
  }

  private static String buildPrivilegeSet(List<String> lists, String value) {
    StringBuilder sb = new StringBuilder();
    for (String str : lists) {
      sb.append(String.format("%s='%s'", str, value));
      sb.append(", ");
    }
    return sb.length() == 0 ? "" : sb.substring(0, sb.length() - 1);
  }

  public static ExecResult execute(String... sqls) {
    return ExecutorHelper.execute(PluginFactory.getSqlExecutor(), PluginFactory.getSqlEngine(),
            PluginFactory.getStoreEngine(), sqls).get(0);
  }
}
