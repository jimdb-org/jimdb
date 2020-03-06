/*
 * Copyright 2019 The ChubaoDB Authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.jimdb.test.mysql.SqlTestBase;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public class UpdateIndexTest extends SqlTestBase {

  private static String DBNAME = "test_update_index";
  private static String INT_TABLENAME = "update_int_idx";
  private static String COMPOSITE_PK_TABLENAME = "update_composite_pk";

  private static TableDataResult INT_TABLE_DATARESULT = new TableDataResult(INT_TABLENAME);
  private static TableDataResult COMPOSITE_PRIMARY_KEY_TABLE_DATARESULT = new TableDataResult(COMPOSITE_PK_TABLENAME);


  @BeforeClass
  public static void init() {
    createDB();
    initCompositePrimaryKeyTable();
  }

  private static void createDB() {
    deleteCatalog(DBNAME);
    createCatalog(DBNAME);
    useCatalog(DBNAME);
  }

  private static void initCompositePrimaryKeyTable() {
    String sql = "CREATE TABLE IF NOT EXISTS `" + COMPOSITE_PK_TABLENAME + "` ( "
            + "`t_bigint` bigint NOT NULL, "
            + "`t_int` int(11) NOT NULL, "
            + "`t_varchar1` varchar(50) NOT NULL, "
            + "`t_varchar2` varchar(50) NOT NULL, "
            + "PRIMARY KEY (`t_bigint`,`t_int`),"
            + "UNIQUE INDEX varchar_idx (`t_varchar1`, `t_varchar2`) "
            + ") COMMENT 'REPLICA=1' ENGINE=MEMORY ";
    dropAndCreateTable(COMPOSITE_PK_TABLENAME, sql);
  }

  @Before
  public void useDB() {
    useCatalog(DBNAME);
  }

  @Test
  public void testUpdateIntPK01() {
    int factor = 200000;
    for (int batch = 1; batch <= 10; batch++) {
      String sql = "INSERT INTO "+ COMPOSITE_PK_TABLENAME
              + " (t_bigint, t_int, t_varchar1, t_varchar2) VALUES";
      sql += "(" + (batch + factor) + ", " + batch + ", 't_varchar1-" + batch + "', 't_varchar2-" + batch + "')";
      execUpdate(sql, 1, true);
    }
    int key = 1;
    String updateSql = String.format("update %s set t_varchar1 = '%s', t_varchar2 = '%s' where t_bigint = %d and t_int = %d",
            COMPOSITE_PK_TABLENAME, "112233-1", "112233-2", (factor + key), key);
    execUpdate(updateSql, 1, true);

    List<String> expected = expectedStr(new String[]{ "t_varchar1=112233-1", "t_varchar2=112233-2" });
    execQuery(String.format("select t_varchar1, t_varchar2 from %s where t_bigint = %d and t_int = %d ",
            COMPOSITE_PK_TABLENAME, (factor + key), key), expected);
  }


  class SelectCols {
    String tableName;
    String[] colNames;
    String select;
    TableDataResult dataResult;


    public SelectCols(TableDataResult dataResult, String[] colNames) {
      this.dataResult = dataResult;
      this.tableName = dataResult.tableName;
      this.colNames = colNames;
      this.select = StringUtils.join(colNames, ",");
    }

    public List<Map<String, Comparable>> getResult() {
      return dataResult.data_result;
    }
  }

  static class TableDataResult {
    String tableName;
    List<Map<String, Comparable>> data_result = new ArrayList<>();
    List<Tuple2<String, Integer>> table_meta = new ArrayList<>();

    public TableDataResult(String tableName) {
      this.tableName = tableName;
    }
  }
}
