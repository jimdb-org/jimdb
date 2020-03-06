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
package io.jimdb.test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.jimdb.common.utils.lang.IOUtil;
import io.jimdb.core.Bootstraps;
import io.jimdb.core.config.JimConfig;
import io.jimdb.sql.server.JimBootstrap;
import io.jimdb.sql.server.netty.JimService;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("COMMAND_INJECTION")
public final class MysqlTestRunner {
  private static final String EXTERN = "--extern";
  private static final String HOST = "127.0.0.1";
  private static String port;
  private static JimService sqlService;

  private MysqlTestRunner() {
  }

  public static void main(String[] args) {
    URL url = MysqlTestRunner.class.getClassLoader().getResource("mysql-test");
    if (url == null) {
      throw new RuntimeException("not found the test script directory 'mysql-test' under classpath");
    }

    int exitCode;
    try {
      startServer();

      ProcessBuilder processBuilder = new ProcessBuilder(buildCommand());
      processBuilder.directory(new File(url.getPath()));
      processBuilder.redirectErrorStream(true);
      processBuilder.inheritIO();

      Process process = processBuilder.start();
      process.waitFor();
      exitCode = process.exitValue();
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      exitCode = 2;
    } finally {
      stopServer();
    }

    System.out.println("\n Test Exit: " + exitCode);
  }

  private static String[] buildCommand() {
    List<String> commands = new ArrayList<>();
    commands.add("perl");
    commands.add("mysql-test-run.pl");
    buildTestOption(commands);

    return commands.toArray(new String[0]);
  }

  private static void buildTestOption(List<String> commands) {
    Properties props = IOUtil.loadResource("mysql_test.properties");

    if (StringUtils.isNotBlank(props.getProperty("test.options", ""))) {
      String option = props.getProperty("test.options");
      while (StringUtils.isNotBlank(option)) {
        if (!option.startsWith("--")) {
          break;
        }

        int pos = option.indexOf("--", 2);
        if (pos < 0) {
          commands.add(option.trim());
          option = null;
        } else {
          commands.add(option.substring(0, pos).trim());
          option = option.substring(pos);
        }
      }
    }

    commands.add(EXTERN);
    commands.add(String.format("host=%s", HOST));
    commands.add(EXTERN);
    commands.add(String.format("port=%s", port));
    commands.add(EXTERN);
    commands.add(String.format("user=%s", props.getProperty("test.user", "root")));
    commands.add(EXTERN);
    commands.add(String.format("password=%s", props.getProperty("test.pass", "root")));

    if (StringUtils.isNotBlank(props.getProperty("test.prefix", ""))) {
      commands.add(String.format("--do-test=%s", props.getProperty("test.prefix")));
    }
    if (StringUtils.isNotBlank(props.getProperty("test.skip", ""))) {
      commands.add(String.format("--skip-test=%s", props.getProperty("test.skip")));
    }
    if (StringUtils.isNotBlank(props.getProperty("test.names", ""))) {
      commands.add(String.format("%s", props.getProperty("test.names")));
    }
  }

  private static void startServer() {
    JimConfig config = Bootstraps.init("jim_test.properties");
    port = String.valueOf(config.getServerConfig().getPort());
    sqlService = JimBootstrap.createServer(config);
    JimBootstrap.start(sqlService);
  }

  private static void stopServer() {
    if (sqlService != null && sqlService.isStarted()) {
      sqlService.stop();
    }
  }
}
