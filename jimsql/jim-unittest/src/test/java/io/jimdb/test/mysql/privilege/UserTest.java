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
package io.jimdb.test.mysql.privilege;

import java.sql.SQLException;
import java.util.List;

import io.jimdb.sql.privilege.ScrambleUtil;
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
    String currTs = String.valueOf(System.nanoTime());
    String seed = currTs.substring(currTs.length() - 10);
    String user = "user_" + seed;

    sql = String.format("CREATE USER '%s'@'localhost';", user);
    execUpdate(sql, 0, true);

    sql = String.format("DROP USER '%s'@'localhost';", user);
    execUpdate(sql, 0, true);

    sql = String.format("CREATE USER '%s'@'localhost' IDENTIFIED BY '123456';", user);
    execUpdate(sql, 0, true);

    sql = String.format("DROP USER '%s'@'localhost';", user);
    execUpdate(sql, 0, true);

    List<String> expected = expectedStr(new String[]{});
    execQuery(String.format("select * from mysql.user where User='%s'", user), expected);

//    sql = "CREATE USER 'test' IDENTIFIED BY '123456';";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER test IDENTIFIED BY '123456';";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER 'test';";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER test;";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER test@;";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER 'test'@'1';";
//    execUpdate(sql, 0, true);
//
//    sql = "CREATE USER '测试';";
//    execUpdate(sql, 0, true);
  }

  @Test
  public void testAlterUser() {
    String currTs = String.valueOf(System.nanoTime());
    String seed = currTs.substring(currTs.length() - 10);
    String user = "user_" + seed;

    sql = String.format("CREATE USER '%s'@'localhost' IDENTIFIED BY '123456';", user);
    execUpdate(sql, 0, true);
    String origin = ScrambleUtil.encodePassword("123456");
    List<String> expected = expectedStr(new String[]{ "Password=" + origin });
    execQuery(String.format("select Password from mysql.user where User='%s'", user), expected);

    sql = String.format("ALTER USER '%s'@'localhost' IDENTIFIED BY 'root' PASSWORD EXPIRE;", user);
    execUpdate(sql, 0, true);

    String modified = ScrambleUtil.encodePassword("root");
    expected = expectedStr(new String[]{ "Password=" + modified });
    execQuery(String.format("select Password from mysql.user where User='%s'", user), expected);

    sql = String.format("DROP USER '%s'@'localhost';", user);
    execUpdate(sql, 0, true);

    expected = expectedStr(new String[]{ });
    execQuery(String.format("select * from mysql.user where User='%s'", user), expected);
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
  public void teatAlterUserError() {
    sql = "ALTER USER ' '@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;";
    result = new SQLException("User variable name ' ' is illegal", "42000", 3061);
    execUpdate(sql, result, true);

    sql = "ALTER USER 'testttttttttttttttttttttttttttttttttttttttt'@'localhost' IDENTIFIED BY '123456' PASSWORD EXPIRE;";
    result = new SQLException("String 'testttttttttttttttttttttttttttttttttttttttt' is too long for user name (should be no longer than 16)", "HY000", 1470);
    execUpdate(sql, result, true);
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
