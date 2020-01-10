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

package io.jimdb.sql.optimizer.statistics;

import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Sample builder is used to build sample for columns (MOCK ONLY).
 * On the data server, we need to implement similar logic.
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD" })
public class SampleBuilder {

  long maxBucketSize;

  long maxSampleSize;

  long maxFMSketchSize;

  int cmSketchDepth;

  int cmSketchWidth;

  /**
   * Collects sample from the result set using Reservoir Sampling algorithm
   * (https://en.wikipedia.org/wiki/Reservoir_sampling), and estimates NDVs using FM Sketch during the process.
   * @return the sample collectors which contain total count, null count, distinct values count and CM Sketch.
   */
  public List<SampleCollector> collectColumnStats() {
    return null;
  }
}
