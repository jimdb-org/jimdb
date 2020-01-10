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
package io.jimdb.test.planner.optimize.suite;

import io.jimdb.test.planner.optimize.integration.LogicalOptimizationTest;
import io.jimdb.test.planner.optimize.integration.PhysicalOptimizationTest;
import io.jimdb.test.planner.optimize.unit.AggregationEliminateTest;
import io.jimdb.test.planner.optimize.unit.AggregationTest;
import io.jimdb.test.planner.optimize.unit.BuildKeyTest;
import io.jimdb.test.planner.optimize.unit.ColumnPruneTest;
import io.jimdb.test.planner.optimize.unit.ConstantPropagatorTest;
import io.jimdb.test.planner.optimize.unit.PredPushDownTest;
import io.jimdb.test.planner.optimize.unit.ProjectionEliminateTest;
import io.jimdb.test.planner.optimize.unit.TopNPushDownTest;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

public class OptimizeSmokeTest {

  public static TestSuite suite() {
    TestSuite suite = new TestSuite("Smoke tests for all plan optimization.");

    // unit optimize
    suite.addTest(new JUnit4TestAdapter(PredPushDownTest.class));
    suite.addTest(new JUnit4TestAdapter(ColumnPruneTest.class));
    suite.addTest(new JUnit4TestAdapter(BuildKeyTest.class));
    suite.addTest(new JUnit4TestAdapter(ProjectionEliminateTest.class));
    suite.addTest(new JUnit4TestAdapter(TopNPushDownTest.class));
    suite.addTest(new JUnit4TestAdapter(ConstantPropagatorTest.class));
    suite.addTest(new JUnit4TestAdapter(AggregationTest.class));
    suite.addTest(new JUnit4TestAdapter(AggregationEliminateTest.class));

    // integration
    suite.addTest(new JUnit4TestAdapter(LogicalOptimizationTest.class));
    suite.addTest(new JUnit4TestAdapter(PhysicalOptimizationTest.class));
    return suite;
  }

}

