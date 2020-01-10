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
package io.jimdb.sql.optimizer;

/**
 * @version V1.0
 */
public class OptimizeFlag {

  public static final int PRUNCOLUMNS = 1 << 0;

  public static final int BUILDKEYINFO = 1 << 1;

  public static final int DECORRELATE = 1 << 2;

  public static final int ELIMINATEAGG = 1 << 3;

  public static final int ELIMINATEPROJECTION = 1 << 4;

  public static final int MAXMINELIMINATE = 1 << 5;

  public static final int PREDICATEPUSHDOWN = 1 << 6;

  public static final int ELIMINATEOUTERJOIN = 1 << 7;

  public static final int PARTITIONPROCESSOR = 1 << 8;

  public static final int PUSHDOWNAGG = 1 << 9;

  public static final int PUSHDOWNTOPN = 1 << 10;

  public static final int JOINREORDER = 1 << 11;
}
