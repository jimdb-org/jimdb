/*
 * Copyright 2019 The Chubao Authors.
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
package io.jimdb.sql.smith;

import io.jimdb.Bootstraps;
import io.jimdb.config.JimConfig;
import io.jimdb.plugin.MetaEngine;
import io.jimdb.sql.smith.cxt.Scope;
import io.jimdb.server.JimBootstrap;
import io.jimdb.server.netty.JimService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * @version V1.0
 */
public class Bootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

  public static void main(String[] args) throws Exception {

    JimConfig jimConfig = Bootstraps.init("jim.properties");
    JimService service = JimBootstrap.createServer(jimConfig);
    JimBootstrap.start(service);
    MetaEngine metaEngine = jimConfig.getMetaEngine();
    ResultChecker checker = new ResultChecker();
    checker.init(jimConfig);
    String catalog = jimConfig.getString("catalog", "test");

    for (int i = 0; i < 100000; i++) {
      Scope scope = new Scope(metaEngine, catalog, Random.getRandomInt(10));
      SQLStatement stmt = scope.makeSQLStmt();
      String stmtSQL = stmt.toString();
      checker.checkResultSet(stmtSQL);
    }
  }
}
