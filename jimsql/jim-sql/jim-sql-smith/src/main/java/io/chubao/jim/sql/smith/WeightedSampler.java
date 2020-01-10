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

import java.util.Random;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "URF_UNREAD_FIELD", "EI_EXPOSE_REP2", "PREDICTABLE_RANDOM" })
public final class WeightedSampler {
  private int[] samples;
  private int[] weights;
  private int randomSeed;
  private Random random;

  public WeightedSampler(int[] weights, int randomSeed) {
    this.weights = weights;
    this.randomSeed = randomSeed;
    this.random = new Random();
    init();
  }

  public void init() {
    int sum = 0;
    for (int weight : weights) {
      if (weight < 1) {
        throw new IllegalArgumentException(weight + " Weight must be greater than or equal to 1");
      }
      sum += weight;
    }
    samples = new int[sum];
    int pos = 0;
    for (int i = 0; i < weights.length; i++) {
      for (int j = 0; j < weights[i]; j++) {
        samples[j] = i;
        pos++;
      }
    }
  }

  public int getNext() {
    return this.samples[random.nextInt(samples.length)];
  }
}
