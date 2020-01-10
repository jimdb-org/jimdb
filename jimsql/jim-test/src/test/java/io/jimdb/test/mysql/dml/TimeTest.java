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
package io.jimdb.test.mysql.dml;

import java.util.Arrays;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Test;

/**
 * @version V1.0
 */
public class TimeTest extends SqlTestBase {
  private static String catalogName = "maggie";
  private static String tableName = "timetest";

  private static final String EXTRA = "comment 'REPLICA =1' \nengine = memory";

  public void createTable() {
    String sql = String.format("create table  %s.%s (id bigint unsigned primary key, " +
                    "name varchar(20), " +
                    "v_time timestamp null ON UPDATE CURRENT_TIMESTAMP, " +
                    "v_time2 timestamp null ON UPDATE CURRENT_TIMESTAMP) " +
                    "AUTO_INCREMENT=0  %s",
            catalogName, tableName, EXTRA);
    execUpdate(sql, 0, true);
  }



  @Test
  public void testBatchInsert() {
//    //delete table
//    execUpdate(String.format("drop table IF EXISTS %s.%s", catalogName, tableName), 0, true);
//
//    createTable();
//    String sql = "INSERT INTO maggie.timetest(id) VALUES(1)";
//    execUpdate(sql, 1, true);
//
//    sql = "select * from maggie.timetest;";
//    List<String> s = execQuery(sql);
//    System.out.println(Arrays.toString(s.toArray()));

    String sql = "update maggie.timetest set name = 'a' where id = 1";
    execUpdate(sql, 1, true);
  }
}
