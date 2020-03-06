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
package io.jimdb.fi;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is responsible for the decision of when a fault has to be triggered.
 * The <pct>% form can be used to specify a probability that fault will execute.
 * This is a decimal in the	range (0, 100].
 * The <cnt>* form can be used to specify the	number of times	that fault should be executed.
 * <p>
 * Note: Only the last probability and the last count are used if multiple are specified.
 * When both a probability and a count are specified, the probability is evaluated before the count.
 *
 * @version V1.0
 */
@SuppressFBWarnings("PREDICTABLE_RANDOM")
abstract class ProbabilityModel {
  protected static final float MIN_PROB = 0.00f; // Min probability is 0%
  protected static final float MAX_PROB = 1.00f; // Max probability is 100%

  protected final int count;
  protected final float prob;
  protected final AtomicInteger counter;

  ProbabilityModel(float prob, int count) {
    this.prob = prob;
    this.count = count;
    this.counter = count < 0 ? null : new AtomicInteger(count);
  }

  /**
   * Check if we have reached the point of injection.
   *
   * @return true if the probability threshold has been reached; false otherwise
   */
  boolean injectCriteria() {
    if (counter != null && counter.get() <= 0) {
      return false;
    }

    if (prob <= MIN_PROB) {
      return false;
    }
    if (prob < MAX_PROB) {
      if (ThreadLocalRandom.current().nextFloat() >= prob) {
        return false;
      }
    }

    if (counter != null) {
      if (counter.get() <= 0 || counter.decrementAndGet() < 0) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProbabilityModel that = (ProbabilityModel) o;
    return count == that.count
            && Float.compare(that.prob, prob) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(count, prob);
  }
}
