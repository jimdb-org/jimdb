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
package io.jimdb.test.planner.statistics.suite;

import io.jimdb.test.planner.statistics.integration.CboIntegrationTest;
import io.jimdb.test.planner.statistics.unit.AggCboTest;
import io.jimdb.test.planner.statistics.unit.BestTaskFinderTest;
import io.jimdb.test.planner.statistics.unit.CboTest;
import io.jimdb.test.planner.statistics.unit.ColumnStatsTest;
import io.jimdb.test.planner.statistics.unit.CountMinSketchTest;
import io.jimdb.test.planner.statistics.unit.HistogramTest;
import io.jimdb.test.planner.statistics.unit.IndexStatsTest;
import io.jimdb.test.planner.statistics.unit.OperatorToTaskAttacherTest;
import io.jimdb.test.planner.statistics.unit.StatUsageTest;
import io.jimdb.test.planner.statistics.unit.StatisticsVisitorTest;
import io.jimdb.test.planner.statistics.unit.TableSourceStatInfoTest;
import io.jimdb.test.planner.statistics.unit.TableStatsTest;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

/**
 * @version V1.0
 */
public class StatsSuiteTest {
  public static TestSuite suite() {
    TestSuite suite = new TestSuite("Smoke tests for all stats.");

    suite.addTest(new JUnit4TestAdapter(BestTaskFinderTest.class));
    suite.addTest(new JUnit4TestAdapter(CboTest.class));
    suite.addTest(new JUnit4TestAdapter(ColumnStatsTest.class));
    suite.addTest(new JUnit4TestAdapter(CountMinSketchTest.class));
    suite.addTest(new JUnit4TestAdapter(HistogramTest.class));
    suite.addTest(new JUnit4TestAdapter(IndexStatsTest.class));
    suite.addTest(new JUnit4TestAdapter(OperatorToTaskAttacherTest.class));
    suite.addTest(new JUnit4TestAdapter(StatisticsVisitorTest.class));
    suite.addTest(new JUnit4TestAdapter(StatUsageTest.class));
    suite.addTest(new JUnit4TestAdapter(TableSourceStatInfoTest.class));
    suite.addTest(new JUnit4TestAdapter(TableStatsTest.class));

    suite.addTest(new JUnit4TestAdapter(CboIntegrationTest.class));
    suite.addTest(new JUnit4TestAdapter(AggCboTest.class));

    return suite;
  }

}
