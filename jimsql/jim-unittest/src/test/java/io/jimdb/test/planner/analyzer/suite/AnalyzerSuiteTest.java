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
package io.jimdb.test.planner.analyzer.suite;

import io.jimdb.test.planner.analyzer.unit.EvaluatorExpTest;
import io.jimdb.test.planner.analyzer.unit.LogicalPlanTest;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

/**
 *
 */
public class AnalyzerSuiteTest {
  public static TestSuite suite() {
    TestSuite suite = new TestSuite("Smoke tests for all analyzer.");

    suite.addTest(new JUnit4TestAdapter(EvaluatorExpTest.class));
    suite.addTest(new JUnit4TestAdapter(LogicalPlanTest.class));

    return suite;
  }
}
