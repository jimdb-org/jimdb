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
package io.jimdb.test.engine.api;

import java.util.Date;
import java.util.Properties;

import io.jimdb.api.JimCluster;
import io.jimdb.api.JimDatabase;
import io.jimdb.core.config.JimConfig;

import org.junit.BeforeClass;

/**
 * @version V1.0
 */
public class JimClusterTest {
  static String catalog = "test";
  static JimCluster cluster;
  static JimDatabase jimDatabase;


  @BeforeClass
  public static void before() {
    Properties properties = new Properties();
    properties.setProperty("jim.meta.address", "http://192.168.183.67:8987");
    properties.setProperty("jim.route.address", "192.168.183.67:18987");
    properties.setProperty("jim.cluster", "8");
    properties.setProperty("mock.flag", "false");
    properties.setProperty("jim.rpc.timeout", "200");
    properties.setProperty("jim.rpc.thread", "1");
    properties.setProperty("netty.client.workerThreads", "1");

    JimConfig config = new JimConfig(properties);

    cluster = new JimCluster();
    cluster.load(config);
    jimDatabase = cluster.getDatabase(catalog);
    System.out.println("start time : " + new Date());
  }

}
