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
package io.jimdb.sql.analyzer.mysql;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.PriLevel;
import io.jimdb.pb.Ddlpb.PriOn;
import io.jimdb.pb.Ddlpb.PriUser;
import io.jimdb.pb.Ddlpb.PrivilegeOp;
import io.jimdb.sql.ddl.DDLUtils;
import io.jimdb.sql.operator.Privilege;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.show.ShowGrants;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;

/**
 * @version V1.0
 */
final class PrivilegeAnalyzer {
  private static final int MAX_LEN_HOSTNAME = 60;
  private static final int MAX_LEN_USERNAME = 16;
  private static final int MAX_LEN_PASSWORD = 41;

  private PrivilegeAnalyzer() {
  }

  static Privilege analyzeGrant(Session session, SQLGrantStatement stmt) {
    if (stmt.isAdminOption()) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_NOT_SUPPORTED_YET, "Temporarily unsupported Admin "
              + "Option");
    }

    if (stmt.getWithGrantOption()) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_NOT_SUPPORTED_YET, "Temporarily unsupported With "
              + "Grant Option");
    }

    if (stmt.getObjectType() != null && ("PROCEDURE".equals(stmt.getObjectType().name) || "FUNCTION".equals(stmt.getObjectType().name))) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_NOT_SUPPORTED_YET, "Temporarily unsupported Procedure");
    }

    List<SQLExpr> sqlExprList = stmt.getPrivileges();
    SQLObject objectOn = stmt.getOn();
    SQLExpr exprTo = stmt.getTo();
    return new Privilege(OpType.PriRevoke, getPrivilegeOp(session, sqlExprList, objectOn, exprTo));
  }

  static Privilege analyzeRevoke(Session session, SQLRevokeStatement stmt) {
    List<SQLExpr> sqlExprList = stmt.getPrivileges();
    SQLObject objectOn = stmt.getOn();
    SQLExpr exprFrom = stmt.getFrom();
    return new Privilege(OpType.PriRevoke, getPrivilegeOp(session, sqlExprList, objectOn, exprFrom));
  }

  static Privilege analyzeCreateUser(MySqlCreateUserStatement stmt) {
    String name = null;
    String host = null;
    String password = null;

    List<MySqlCreateUserStatement.UserSpecification> users = stmt.getUsers();
    List<PriUser> createUsers = new ArrayList<>(users.size());

    for (MySqlCreateUserStatement.UserSpecification user : users) {
      SQLExpr userExpr = user.getUser();

      if (userExpr instanceof MySqlUserName) {
        // CREATE USER 'dog'@'localhost' IDENTIFIED BY '123456';
        MySqlUserName mySqlUserName = (MySqlUserName) userExpr;
        name = mySqlUserName.getUserName();
        host = mySqlUserName.getHost();
        password = mySqlUserName.getIdentifiedBy();
      } else if (userExpr instanceof SQLIdentifierExpr) {
        // CREATE USER dog IDENTIFIED BY '123456';
        SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) userExpr;
        name = sqlIdentifierExpr.getName();
        SQLExpr passExpr = user.getPassword();
        if (passExpr instanceof SQLCharExpr) {
          password = ((SQLCharExpr) passExpr).getText();
        }
      } else if (userExpr instanceof SQLCharExpr) {
        // CREATE USER 'dog' IDENTIFIED BY '123456';
        SQLCharExpr sqlCharExpr = (SQLCharExpr) userExpr;
        name = sqlCharExpr.getText();
        SQLExpr passExpr = user.getPassword();
        if (passExpr instanceof SQLCharExpr) {
          password = ((SQLCharExpr) passExpr).getText();
        }
      }

      createUsers.add(checkPriUser(name, host, password));
    }

    PrivilegeOp.Builder builder = PrivilegeOp.newBuilder();
    builder.addAllUsers(createUsers);

    return new Privilege(OpType.PriCreateUser, builder.build());
  }

  static Privilege analyzeDropUser(SQLDropUserStatement stmt) {
    String name = null;
    String host = null;

    List<SQLExpr> users = stmt.getUsers();
    List<PriUser> dropUsers = new ArrayList<>(users.size());

    for (SQLExpr userExpr : users) {
      if (userExpr instanceof MySqlUserName) {
        // DROP USER 'dog'@'localhost';
        MySqlUserName mySqlUserName = (MySqlUserName) userExpr;
        name = mySqlUserName.getUserName();
        host = mySqlUserName.getHost();
      } else if (userExpr instanceof SQLIdentifierExpr) {
        // DROP USER dog;
        SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) userExpr;
        name = sqlIdentifierExpr.getName();
      } else if (userExpr instanceof SQLCharExpr) {
        // DROP USER 'dog';
        SQLCharExpr sqlCharExpr = (SQLCharExpr) userExpr;
        name = sqlCharExpr.getText();
      }

      dropUsers.add(checkPriUser(name, host, ""));
    }

    PrivilegeOp.Builder builder = PrivilegeOp.newBuilder();
    builder.addAllUsers(dropUsers);

    return new Privilege(OpType.PriDropUser, builder.build());
  }

  static Privilege analyzeAlterUser(MySqlAlterUserStatement stmt) {
    String name = null;
    String host = null;
    String password = null;

    List<SQLExpr> users = stmt.getUsers();
    List<PriUser> alterUsers = new ArrayList<>(users.size());

    for (SQLExpr userExpr : users) {
      if (userExpr instanceof MySqlUserName) {
        // ALTER USER 'dog'@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;
        MySqlUserName mySqlUserName = (MySqlUserName) userExpr;
        name = mySqlUserName.getUserName();
        host = mySqlUserName.getHost();
        password = mySqlUserName.getIdentifiedBy();
      }

      alterUsers.add(checkPriUser(name, host, password));
    }

    PrivilegeOp.Builder builder = PrivilegeOp.newBuilder();
    builder.addAllUsers(alterUsers);

    return new Privilege(OpType.PriUpdateUser, builder.build());
  }

  static Privilege analyzeSetPass(SQLSetStatement stmt) {
    String name = null;
    String host = null;
    String password = null;

    List<SQLAssignItem> items = stmt.getItems();
    List<PriUser> users = new ArrayList<>(items.size());

    for (SQLAssignItem item : items) {
      SQLExpr target = item.getTarget();
      if (target instanceof MySqlUserName) {
        host = ((MySqlUserName) target).getHost();
        name = ((MySqlUserName) target).getUserName();
      }

      SQLExpr value = item.getValue();
      if (value instanceof SQLCharExpr) {
        password = ((SQLCharExpr) value).getText();
      }

      users.add(checkPriUser(name, host, password));
    }

    PrivilegeOp.Builder builder = PrivilegeOp.newBuilder();
    builder.addAllUsers(users);

    return new Privilege(OpType.PriSetPassword, builder.build());
  }

  static Privilege analyzeFlush(MySqlFlushStatement stmt) {
    if (stmt.isPrivileges()) {
      return new Privilege(OpType.PriFlush, null);
    }
    throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_NOT_SUPPORTED_YET, "Flush");
  }

  static RelOperator analyzeShowGrants(MySqlShowGrantsStatement stmt) {
    String user = null;
    String host = null;
    SQLExpr sqlExpr = stmt.getUser();
    if (sqlExpr instanceof MySqlUserName) {
      user = ((MySqlUserName) sqlExpr).getUserName();
      host = ((MySqlUserName) sqlExpr).getHost();
    }

    return new ShowGrants(user, host);
  }

  private static PriUser checkPriUser(String name, String host, String password) {
    name = DDLUtils.trimName(name);
    host = DDLUtils.trimName(host);
    password = DDLUtils.trimName(password);

    if (StringUtils.isBlank(name)) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_ILLEGAL_USER_VAR, name);
    }
    if (name.length() > MAX_LEN_USERNAME) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_WRONG_STRING_LENGTH, name, "user name",
              String.valueOf(MAX_LEN_USERNAME));
    }
    if (StringUtils.isBlank(host)) {
      host = "%";
    }
    if (host.length() > MAX_LEN_HOSTNAME) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_WRONG_STRING_LENGTH, host, "host name",
              String.valueOf(MAX_LEN_HOSTNAME));
    }
    if (StringUtils.isBlank(password)) {
      password = "";
    }
    if (password.length() > MAX_LEN_PASSWORD) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_WRONG_STRING_LENGTH, password, "password",
              String.valueOf(MAX_LEN_PASSWORD));
    }

    PriUser.Builder userBuilder = PriUser.newBuilder();
    userBuilder.setName(name)
            .setHost(host)
            .setPassword(password);

    return userBuilder.build();
  }

  private static PriOn.Builder getPriOnBuilder(String db, String table) {
    PriOn.Builder onBuilder = PriOn.newBuilder();

    if ("*".equals(db) && "*".equals(table)) {
      return onBuilder.setLevel(PriLevel.Global);
    } else if (!"*".equals(db) && "*".equals(table)) {
      return onBuilder.setLevel(PriLevel.DB)
              .setDb(db);
    } else {
      return onBuilder.setLevel(PriLevel.Table)
              .setDb(db)
              .setTable(table);
    }
  }

  private static PrivilegeOp getPrivilegeOp(Session session, List<SQLExpr> sqlExprList, SQLObject objectOn,
                                            SQLExpr expr) {
    List<String> privList = new ArrayList<>();
    for (SQLExpr sqlExpr : sqlExprList) {
      if (sqlExpr instanceof SQLIdentifierExpr) {
        String priv = ((SQLIdentifierExpr) sqlExpr).getName();
        privList.add(priv);
      }
    }

    String db = null;
    String table = null;
    if (objectOn instanceof SQLExprTableSource) {
      SQLExpr exprOn = ((SQLExprTableSource) objectOn).getExpr();
      if (exprOn instanceof SQLPropertyExpr) {
        SQLExpr owner = ((SQLPropertyExpr) exprOn).getOwner();
        if (owner instanceof SQLAllColumnExpr) {
          db = "*";
        } else if (owner instanceof SQLIdentifierExpr) {
          db = ((SQLIdentifierExpr) owner).getName();
        }
        table = ((SQLPropertyExpr) exprOn).getName();
      } else if (exprOn instanceof SQLAllColumnExpr) {
        db = session.getVarContext().getDefaultCatalog();
        table = "*";
      }
    }

    List<PriUser> users = new ArrayList<>();
    String name = null;
    String host = null;
    String password = null;
    if (expr instanceof MySqlUserName) {
      host = ((MySqlUserName) expr).getHost();
      name = ((MySqlUserName) expr).getUserName();
      password = ((MySqlUserName) expr).getIdentifiedBy();
    } else if (expr instanceof SQLIdentifierExpr) {
      name = ((SQLIdentifierExpr) expr).getName();
    }

    users.add(checkPriUser(name, host, password));

    PrivilegeOp.Builder builder = PrivilegeOp.newBuilder();
    builder.addAllPrivileges(privList)
            .setOn(getPriOnBuilder(db, table))
            .addAllUsers(users);

    return builder.build();
  }
}
