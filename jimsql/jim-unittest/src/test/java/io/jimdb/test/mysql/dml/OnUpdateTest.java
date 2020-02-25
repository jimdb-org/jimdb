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
import java.util.TimeZone;

import io.jimdb.core.values.DateValue;
import io.jimdb.pb.Basepb;
import io.jimdb.test.mysql.SqlTestBase;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class OnUpdateTest extends SqlTestBase {
  private static final String EXTRA = "comment 'REPLICA = 1' engine = memory";
  private static final String TABLE_NAME = "time_test";

  @Test
  public void testUpdateForIndex() {
    String DB_NAME = "on_update_test_db" + System.nanoTime();
    createCatalog(DB_NAME);
    String sql = String.format("create table %s.%s (id bigint unsigned primary key, " +
            "name varchar(20), " +
            "v_time timestamp null ON UPDATE CURRENT_TIMESTAMP, " +
            "v_time2 timestamp null ON UPDATE CURRENT_TIMESTAMP," +
            "UNIQUE index timeidx(v_time) )" +
            "AUTO_INCREMENT=0 %s", DB_NAME, TABLE_NAME, EXTRA);
    execUpdate(sql, 0, true);

    sql = String.format("INSERT INTO %s.%s(id) VALUES(1)", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    List<String> expected = expectedStr(new String[]{ "id=1; name=null; v_time=null; v_time2=null" });
    execQuery(String.format("select * from %s.%s", DB_NAME, TABLE_NAME), expected);

    sql = String.format("update %s.%s set name = 'a' where id = 1", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    sql = String.format("select v_time from %s.%s;", DB_NAME, TABLE_NAME);
    List<String> execResult = execQuery(sql);
    String resultString = execResult.get(0);
    resultString = resultString.substring(7);
    Assert.assertNotNull(resultString);

    sql = String.format("select v_time from %s.%s where v_time = '%s'", DB_NAME, TABLE_NAME, resultString);
    execResult = execQuery(sql);
    String resultString2 = execResult.get(0);
    resultString2 = resultString2.substring(7);

    Assert.assertEquals(resultString, resultString2);

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testUpdateTimestampOfOnUpdate() {
    String DB_NAME = "on_update_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = String.format("create table if not exists %s.%s (id bigint unsigned primary key, " +
                    "name varchar(20), " +
                    "v_time timestamp null ON UPDATE CURRENT_TIMESTAMP, " +
                    "v_time2 timestamp null ON UPDATE CURRENT_TIMESTAMP) " +
                    "AUTO_INCREMENT=0 %s",
            DB_NAME, TABLE_NAME, EXTRA);
    execUpdate(sql, 0, true);

    sql = String.format("INSERT INTO %s.%s(id) VALUES(1)", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    List<String> expected = expectedStr(new String[]{ "id=1; name=null; v_time=null; v_time2=null" });
    execQuery(String.format("select * from %s.%s", DB_NAME, TABLE_NAME), expected);

    // UTC Time
    DateValue beforeUpdateTs = DateValue.getNow(Basepb.DataType.TimeStamp, 0, TimeZone.getDefault());
    System.out.println(beforeUpdateTs.convertToString(null));

    sql = String.format("update %s.%s set name = 'a' where id = 1", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    sql = String.format("select * from %s.%s;", DB_NAME, TABLE_NAME);
    List<String> execResult = execQuery(sql);
    System.out.println(Arrays.toString(execResult.toArray()));

    String[] fields = execResult.get(0).split(";");
    Assert.assertEquals("id=1", fields[0].trim());
    Assert.assertEquals("name=a", fields[1].trim());

    // UTC Time
    DateValue ts1 = DateValue.getInstance(StringUtils.substringAfter(fields[2], "v_time="),
            Basepb.DataType.TimeStamp, 1, TimeZone.getDefault());
    DateValue ts2 = DateValue.getInstance(StringUtils.substringAfter(fields[3], "v_time2="),
            Basepb.DataType.TimeStamp, 1, TimeZone.getDefault());
    Assert.assertTrue(ts1.compareTo(null, beforeUpdateTs) >= 0);
    Assert.assertTrue(ts2.compareTo(null, beforeUpdateTs) >= 0);

    // should not update timestamp
    sql = String.format("update %s.%s set name = 'a' where id = 1", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);
    sql = String.format("select * from %s.%s;", DB_NAME, TABLE_NAME);
    List<String> lastExecResult = execQuery(sql);
    Assert.assertEquals(Arrays.toString(execResult.toArray()), Arrays.toString(lastExecResult.toArray()));

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testUpdateOtherFieldOfOnUpdate() {
    String DB_NAME = "on_update_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = String.format("create table if not exists %s.%s (id bigint unsigned primary key, " +
                    "name varchar(20), " +
                    "v_time timestamp null ON UPDATE CURRENT_TIMESTAMP) " +
                    "AUTO_INCREMENT=0 %s",
            DB_NAME, TABLE_NAME, EXTRA);
    execUpdate(sql, 0, true);

    sql = String.format("INSERT INTO %s.%s(id) VALUES(1)", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    List<String> expected = expectedStr(new String[]{ "id=1; name=null; v_time=null" });
    execQuery(String.format("select * from %s.%s", DB_NAME, TABLE_NAME), expected);

    sql = String.format("update %s.%s set name = 'a', v_time='2020-01-15 18:32:19' where id = 1", DB_NAME, TABLE_NAME);
    execUpdate(sql, 1, true);

    expected = expectedStr(new String[]{ "id=1; name=a; v_time=2020-01-15 18:32:19.0" });
    execQuery(String.format("select * from %s.%s", DB_NAME, TABLE_NAME), expected);

    deleteCatalog(DB_NAME);
  }
}
