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
package io.jimdb.test.mysql.privilege;

import java.sql.SQLException;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Test;

/**
 * UserTest
 *
 * @since 2019/12/10
 */
public class UserTest extends SqlTestBase {

  String sql = null;
  SQLException result = null;

  @Test
  public void testCreateUser() {
    sql = "CREATE USER 'test'@'localhost';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER 'test'@'localhost' IDENTIFIED BY '123456';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER 'test' IDENTIFIED BY '123456';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER test IDENTIFIED BY '123456';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER 'test';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER test;";
    execUpdate(sql, 0, true);

    sql = "CREATE USER test@;";
    execUpdate(sql, 0, true);

    sql = "CREATE USER 'test'@'1';";
    execUpdate(sql, 0, true);

    sql = "CREATE USER '测试';";
    execUpdate(sql, 0, true);
  }

  @Test
  public void testCreateUserError() {
    sql = "CREATE USER ' ';";
    result = new SQLException("User variable name ' ' is illegal", "42000", 3061);
    execUpdate(sql, result, true);

    sql = "CREATE USER testttttttttttttttttttttttttttttttttttttttt;";
    result = new SQLException("String 'testttttttttttttttttttttttttttttttttttttttt' is too long for user name (should be no longer than 16)", "HY000", 1470);
    execUpdate(sql, result, true);

    sql = "CREATE USER 'test'@'localhosttttttttttttttttttttttttttttttttttttttttttttttttttttt';";
    result = new SQLException("String 'localhosttttttttttttttttttttttttttttttttttttttttttttttttttttt' is too long for host name (should be no longer than 60)", "HY000", 1470);
    execUpdate(sql, result, true);

//    sql = "CREATE USER '💩';";
//    result = new SQLException("This version of MySQL doesn't yet support 'Operate system database 'mysql''", "42000", 1235);
//    execUpdate(sql, result, true);
  }

  @Test
  public void testAlterUser() {
    sql = "ALTER USER 'test'@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;";
    execUpdate(sql, 0, true);
  }

  @Test
  public void teatAlterUserError() {
    sql = "ALTER USER ' '@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;";
    result = new SQLException("User variable name ' ' is illegal", "42000", 3061);
    execUpdate(sql, result, true);

    sql = "ALTER USER 'testttttttttttttttttttttttttttttttttttttttt'@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;";
    result = new SQLException("String 'testttttttttttttttttttttttttttttttttttttttt' is too long for user name (should be no longer than 16)", "HY000", 1470);
    execUpdate(sql, result, true);
  }

  @Test
  public void testDropUser() {
    sql = "DROP USER 'test'@'localhost';";
    execUpdate(sql, 0, true);

    sql = "DROP USER 'test';";
    execUpdate(sql, 0, true);

    sql = "DROP USER test;";
    execUpdate(sql, 0, true);
  }

  @Test
  public void teatDropUserError() {
    sql = "DROP USER ' ';";
    result = new SQLException("User variable name ' ' is illegal", "42000", 3061);
    execUpdate(sql, result, true);

    sql = "DROP USER 'testttttttttttttttttttttttttttttttttttttttt';";
    result = new SQLException("String 'testttttttttttttttttttttttttttttttttttttttt' is too long for user name (should be no longer than 16)", "HY000", 1470);
    execUpdate(sql, result, true);
  }
}
