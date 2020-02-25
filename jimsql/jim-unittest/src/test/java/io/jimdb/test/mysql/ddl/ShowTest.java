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
package io.jimdb.test.mysql.ddl;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class ShowTest extends SqlTestBase {

  @Test
  public void testShowCharacter() {
    List<String> expected = expectedStr(new String[]{ "Charset=utf8; Description=utf8; Default collation=utf8; Maxlen=2147483647" });
    execQuery("show character set", expected);
  }

  @Test
  public void testShowCollation() {
    List<String> expected = expectedStr(new String[]{ "Collation=utf8; Charset=utf8; Id=1; Default=Yes; Compiled=Yes; Sortlen=1" });
    execQuery("SHOW COLLATION", expected);
  }

  @Test
  public void testShowDatabases() {
    List<String> dbs = new ArrayList<>(22);
    dbs.add("show_test_db" + System.nanoTime());
    dbs.add("show_test_db" + System.nanoTime());
    dbs.add("show_test_db" + System.nanoTime());
    createCatalog(dbs.get(0));
    createCatalog(dbs.get(1));
    createCatalog(dbs.get(2));

    List<String> results = new ArrayList<>();
    execQuery("SHOW DATABASES", rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString("Database"));
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    });
    int found = 0;
    for (String str : results) {
      if (dbs.contains(str.toLowerCase())) {
        found++;
      }
    }
    Assert.assertEquals(dbs.size(), found);

    deleteCatalog(dbs.get(0));
    deleteCatalog(dbs.get(1));
    deleteCatalog(dbs.get(2));
  }

  @Test
  public void testShowTables() {
    String DB_NAME = "show_test_db" + System.nanoTime();
    createCatalog(DB_NAME);
    String sql = String.format("use %s", DB_NAME);
    execUpdate(sql, 0, true);

    List<String> tables = new ArrayList<>();
    tables.add("test1");
    tables.add("test2");
    sql = String.format("CREATE TABLE %s.test1(id BIGINT PRIMARY KEY, user varchar(32) NOT NULL UNIQUE KEY, host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY", DB_NAME);
    execUpdate(sql, 0, true);
    sql = String.format("CREATE TABLE %s.test2(id BIGINT PRIMARY KEY) COMMENT 'REPLICA=1' ENGINE=MEMORY", DB_NAME);
    execUpdate(sql, 0, true);

    List<String> results = new ArrayList<>();
//    sql = String.format("show tables");
    sql = String.format("show tables from %s", DB_NAME);
    execQuery(sql, rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString(String.format("Tables_in_%s", DB_NAME)));
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    });
    int found = 0;
    for (String str : results) {
      if (tables.contains(str.toLowerCase())) {
        found++;
      }
    }
    Assert.assertEquals(tables.size(), found);

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testShowIndex() {
    String DB_NAME = "show_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = String.format("CREATE TABLE %s.test(id BIGINT PRIMARY KEY, user varchar(32) NOT NULL, "
            + "host varchar(32), UNIQUE INDEX user_idx (user), INDEX host_idx (host)) COMMENT 'REPLICA=1' ENGINE=MEMORY", DB_NAME);
//    String sql = String.format("CREATE TABLE IF NOT EXISTS %s.test_index4 (\n"
//            + "            `i1` int(11) NOT NULL,\n"
//            + "            `i2` int(11) NOT NULL,\n"
//            + "            `i3` int(11) NOT NULL,\n"
//            + "            `b1` bigint NOT NULL,\n"
//            + "            `b2` bigint DEFAULT NULL,\n"
//            + "            `b3` bigint NOT NULL,\n"
//            + "            `v1` varchar(100) NOT NULL,\n"
//            + "            `v2` varchar(100) NOT NULL,\n"
//            + "            `v3` varchar(100) NOT NULL,\n"
//            + "            `ti1` tinyint NOT NULL,\n"
//            + "            `ti2` tinyint NOT NULL,\n"
//            + "            `ti3` tinyint NOT NULL,\n"
//            + "            `si1` smallint NOT NULL,\n"
//            + "            `si2` smallint NOT NULL,\n"
//            + "            `si3` smallint NOT NULL,\n"
//            + "            `mi1` mediumint NOT NULL,\n"
//            + "            `mi2` mediumint NOT NULL,\n"
//            + "            `mi3` mediumint NOT NULL,\n"
//            + "            PRIMARY KEY (i1,i2,i3),\n"
//            + "            INDEX bigint_idx (b1,b2,b3),\n"
//            + "            INDEX varchar_idx (v1,v2,v3),\n"
//            + "            INDEX smallint_idx (si1,si2,si3),\n"
//            + "            INDEX mediumint_idx (mi1,mi2,mi3),\n"
//            + "            INDEX tinyint_idx (ti1,ti2,ti3),\n"
//            + "INDEX ibv_composite_uniq_idx (i3,b3,v3),\n"
//            + "INDEX mst_normal_idx (mi1,si1,ti1),\n"
//            + "INDEX ibv_normal_idx (i1,b1,v1),\n"
//            + "UNIQUE INDEX mst_composite_uniq_idx (mi3,si3,ti3)\n"
//            + "            ) COMMENT 'REPLICA=1' ENGINE=MEMORY AUTO_INCREMENT=0;", DB_NAME);

    execUpdate(sql, 0, true);

//    sql = String.format("SHOW INDEXES FROM %s.test", DB_NAME);
//    sql = String.format("SHOW INDEX FROM test FROM %s", DB_NAME);
    sql = String.format("SHOW INDEXES FROM test IN %s", DB_NAME);
    List<String> expected = expectedStr(new String[]{
            "Table=test; Non_unique=0; Key_name=PRIMARY; Seq_in_index=1; Column_name=id; Collation=A; Cardinality=0; Sub_part=null; Packed=null; Null=; Index_type=BTREE; Comment=; Index_comment",
            "Table=test; Non_unique=0; Key_name=user_2; Seq_in_index=1; Column_name=user; Collation=A; Cardinality=0; Sub_part=1; Packed=null; Null=; Index_type=BTREE; Comment=" });
    execQuery(sql, expected, false);

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testShowColumns01() {
    String DB_NAME = "show_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = String.format("CREATE TABLE %s.test(id BIGINT PRIMARY KEY, user varchar(32) NOT NULL UNIQUE KEY, "
            + "host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY", DB_NAME);
    execUpdate(sql, 0, true);

    List<String> expected = expectedStr(new String[]{
            "Field=id; Type=bigint(20); Null=NO; Key=PRI; Default=null; Extra=",
            "Field=user; Type=varchar(32); Null=NO; Key=UNI; Default=null; Extra=",
            "Field=host; Type=varchar(32); Null=YES; Key=; Default=null; Extra="
    });
    execQuery("show columns from test in " + DB_NAME, expected);

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testShowColumns02() {
    String DB_NAME = "show_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = "CREATE TABLE IF NOT EXISTS " + DB_NAME + ".test ("
            + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
            + "t_char1 char(0) DEFAULT NULL,"
            + "t_char2 char(255)  DEFAULT NULL,"
            + "t_varchar1 varchar(0)  DEFAULT NULL,"
            + "t_varchar2 varchar(65535)  DEFAULT NULL"
            + ")COMMENT 'REPLICA=1' ENGINE=memory AUTO_INCREMENT=0;";
    execUpdate(sql, 0, true);

    List<String> expected = expectedStr(new String[]{
            "Field=id; Type=bigint(20) unsigned; Null=NO; Key=PRI; Default=null; Extra=auto_increment",
            "Field=t_char1; Type=char(0); Null=YES; Key=; Default=null; Extra=",
            "Field=t_char2; Type=char(255); Null=YES; Key=; Default=null; Extra=",
            "Field=t_varchar1; Type=varchar(0); Null=YES; Key=; Default=null; Extra=",
            "Field=t_varchar2; Type=varchar(65535); Null=YES; Key=; Default=null; Extra=",
    });
    execQuery("show columns from test in " + DB_NAME, expected);

    deleteCatalog(DB_NAME);
  }

  @Test
  public void testShowCreateTable() {
    String DB_NAME = "show_test_db" + System.nanoTime();
    createCatalog(DB_NAME);

    String sql = String.format("CREATE TABLE %s.test(id BIGINT PRIMARY KEY, user varchar(32) NOT NULL UNIQUE KEY, "
            + "host varchar(32)) COMMENT 'REPLICA=1' ENGINE=MEMORY", DB_NAME);
    execUpdate(sql, 0, true);

    List<String> expected = expectedStr(new String[]{
            "Table=test; Create Table=CREATE TABLE `test`(\n" +
                    " `id` bigint(20) NOT NULL,\n" +
                    " `user` varchar(32) NOT NULL,\n" +
                    " `host` varchar(32),\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  UNIQUE KEY `user_2`(`user`)\n" +
                    ") ENGINE=MEMORY COMMENT='REPLICA=1'"
    });
    execQuery(String.format("show create table %s.test", DB_NAME), expected);

    deleteCatalog(DB_NAME);
  }
}
