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

import io.jimdb.core.Bootstraps;
import io.jimdb.core.config.JimConfig;
import io.jimdb.sql.server.JimBootstrap;
import io.jimdb.sql.server.netty.JimService;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @version V1.0
 */
public class ProxyStartup {
  protected static JimService service;
  protected static HikariDataSource dataSource;

  public static void main(String[] args) throws Exception {
    if (dataSource == null) {
      JimConfig config = Bootstraps.init("jim_test.properties");

      // disable background thread of fetching stats info
      config.setEnableBackgroundStatsCollector(false);

      service = JimBootstrap.createServer(config);
      JimBootstrap.start(service);
    }
  }
}
