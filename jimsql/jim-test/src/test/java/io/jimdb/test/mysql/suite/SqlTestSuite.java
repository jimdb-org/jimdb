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
package io.jimdb.test.mysql.suite;

import io.jimdb.test.mysql.ddl.CatalogTest;
import io.jimdb.test.mysql.ddl.ColumnTest;
import io.jimdb.test.mysql.ddl.ReOrgTest;
import io.jimdb.test.mysql.ddl.ShowTest;
import io.jimdb.test.mysql.ddl.TableTest;
import io.jimdb.test.mysql.dml.AutoCommitTest;
import io.jimdb.test.mysql.dml.CodecTest;
import io.jimdb.test.mysql.dml.ExplainTest;
import io.jimdb.test.mysql.dml.InsertTest;
import io.jimdb.test.mysql.dml.KeyDeleteTest;
import io.jimdb.test.mysql.dml.KeyGetTest;
import io.jimdb.test.mysql.dml.KeyUpdateTest;
import io.jimdb.test.mysql.dml.MyDecimalSqlTest;
import io.jimdb.test.mysql.dml.OnUpdateTest;
import io.jimdb.test.mysql.dml.PreparedStmtTest;
import io.jimdb.test.mysql.dml.SelectAggTest;
import io.jimdb.test.mysql.dml.SelectIndexTest;
import io.jimdb.test.mysql.dml.SelectTest;
import io.jimdb.test.mysql.dml.SortTest;
import io.jimdb.test.mysql.dml.ValueCheckerTest;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

/**
 * @version V1.0
 */
public class SqlTestSuite {

  public static TestSuite suite() {
    TestSuite suite = new TestSuite("Smoke tests for all sql test which data is from data-server.");

    // unit optimize
    // ddl
    suite.addTest(new JUnit4TestAdapter(CatalogTest.class));
    suite.addTest(new JUnit4TestAdapter(ColumnTest.class));
    suite.addTest(new JUnit4TestAdapter(ReOrgTest.class));
    suite.addTest(new JUnit4TestAdapter(ShowTest.class));
    suite.addTest(new JUnit4TestAdapter(TableTest.class));

    // dml
    suite.addTest(new JUnit4TestAdapter(AutoCommitTest.class));
    suite.addTest(new JUnit4TestAdapter(CodecTest.class));
    suite.addTest(new JUnit4TestAdapter(InsertTest.class));
    suite.addTest(new JUnit4TestAdapter(InsertTest.class));
    suite.addTest(new JUnit4TestAdapter(KeyUpdateTest.class));
    suite.addTest(new JUnit4TestAdapter(KeyGetTest.class));
    suite.addTest(new JUnit4TestAdapter(KeyDeleteTest.class));
    suite.addTest(new JUnit4TestAdapter(PreparedStmtTest.class));
    suite.addTest(new JUnit4TestAdapter(SelectTest.class));
    suite.addTest(new JUnit4TestAdapter(SelectIndexTest.class));
    suite.addTest(new JUnit4TestAdapter(SelectAggTest.class));
    suite.addTest(new JUnit4TestAdapter(SortTest.class));
    suite.addTest(new JUnit4TestAdapter(ExplainTest.class));
    suite.addTest(new JUnit4TestAdapter(MyDecimalSqlTest.class));
    suite.addTest(new JUnit4TestAdapter(OnUpdateTest.class));
    suite.addTest(new JUnit4TestAdapter(ValueCheckerTest.class));
    return suite;
  }

}
