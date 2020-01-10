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
package io.jimdb.rpc.client;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.JUnit4TestAdapter;

/**
 * @version V1.0
 */
public class SuiteTest {
  public static Test suite() {
    TestSuite suite = new TestSuite("rpc suite test");

    suite.addTest(new JUnit4TestAdapter(ConnectEventListenerTest.class));
    suite.addTest(new JUnit4TestAdapter(ConnectPoolTest.class));
    suite.addTest(new JUnit4TestAdapter(HeartbeatListenerTest.class));
    suite.addTest(new JUnit4TestAdapter(NettyClientTest.class));

    return suite;
  }
}
