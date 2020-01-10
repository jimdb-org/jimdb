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
package io.jimdb.mysql.constant;

/**
 * @version V1.0
 */
public final class MySQLVersion {

  //TODO config

  /**
   * MySQL source /include/mysql_version.h
   */
  public static final int PROTOCOL_VERSION_41 = 0x0A;

  public static final int PROTOCOL_VERSION_320 = 0x09;


  /**
   * MySQL source /include/mysql_version.h
   */
  public static final String MYSQL_SERVER_VERSION = "5.6.0";

  public static final int CHARSET = 0x21;
}
