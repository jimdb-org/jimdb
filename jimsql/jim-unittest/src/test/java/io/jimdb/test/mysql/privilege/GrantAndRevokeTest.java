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

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Test;

/**
 * @since 2019/12/25
 */
public class GrantAndRevokeTest extends SqlTestBase {
  String sql = null;
  SQLException result = null;

  @Test
  public void testGrantAll() {
    sql = "grant all privileges on *.* to 'cba'@'%' identified by '123456'";
    execUpdate(sql, 0, true);

    sql = "SELECT * FROM mysql.user where host = '%' and user = 'cba'";
    List<String> expected = expectedStr(new String[]{ "User=cba" });
    execQuery(sql, expected, false);
  }

  @Test
  public void testDropUser() {
    sql = "DROP USER 'jdtest'@'%'";
    execUpdate(sql, 0, true);

    sql = "SELECT user FROM mysql.user";
    List<String> expected = expectedStr(new String[]{ "User='dba'" });
    execQuery(sql, expected, false);
  }

  @Test
  public void testDelete() {
//    sql = "show databases";
//    execUpdate(sql, 0, true);

    sql = "show databases";
    List<String> expected = expectedStr(new String[]{ "Database='dba'" });
    execQuery(sql, expected, false);
  }



}
