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

import io.jimdb.test.mysql.dml.ExplainTest;
import io.jimdb.test.mysql.dml.InsertTest;
import io.jimdb.test.mysql.dml.KeyDeleteTest;
import io.jimdb.test.mysql.dml.KeyGetTest;
import io.jimdb.test.mysql.dml.KeyUpdateTest;
import io.jimdb.test.mysql.dml.MyDecimalSqlTest;
import io.jimdb.test.mysql.dml.PreparedStmtTest;
import io.jimdb.test.mysql.dml.SelectAggTest;
import io.jimdb.test.mysql.dml.SelectIndexTest;
import io.jimdb.test.mysql.dml.SelectTest;
import io.jimdb.test.mysql.dml.SortTest;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

/**
 * @version V1.0
 */
public class SqlTestSuite {

  public static TestSuite suite() {
    TestSuite suite = new TestSuite("Smoke tests for all sql test which data is from data-server.");

    // unit optimize
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
    return suite;
  }

}
