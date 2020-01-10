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
package io.jimdb.common.utils.retry;

import java.time.Instant;

import io.jimdb.common.utils.os.SystemClock;

/**
 * @version V1.0
 */
public final class RetryPolicy {
  public static final int DEFAULT_RETRY_DELAY = 200;

  public static final int DEFAULT_MAX_RETRY_DELAY = 10 * 1000;

  public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.2;

  public static final int DEFAULT_MAX_RETRY = 0;

  public static final int DEFAULT_EXPIRE_TIME = 0;

  // retry delay(ms)
  private final int retryDelay;
  // max retry delay(ms)
  private final int maxRetryDelay;
  // max retry(0=infinite)
  private final int maxRetry;
  // expire time
  private final int expireTime;
  // use exponential
  private final boolean useExponentialBackOff;
  // exponential multiplier(>=1)
  private final double backOffMultiplier;
  // max exponential
  private final int maxExponential;

  private RetryPolicy(int retryDelay, int maxRetryDelay, int maxRetry, int expireTime, boolean useExponentialBackOff, double backOffMultiplier) {
    this.retryDelay = retryDelay;
    this.maxRetryDelay = maxRetryDelay;
    this.maxRetry = maxRetry;
    this.expireTime = expireTime;
    this.useExponentialBackOff = useExponentialBackOff;
    this.backOffMultiplier = backOffMultiplier;
    this.maxExponential = (int) (Math.log(maxRetryDelay) / Math.log(backOffMultiplier));
  }

  /**
   * Return retry delay time(ms).
   * IF return <0, then retry end.
   *
   * @param startTime
   * @param retryTimes
   * @return
   */
  public long getDelay(final long startTime, final int retryTimes) {
    final int retrys = retryTimes < 1 ? 1 : retryTimes;
    if (maxRetry > 0 && retrys > maxRetry) {
      return -1;
    }

    long time = 0;
    if (retryDelay > 0) {
      long delay = 0;
      if (useExponentialBackOff) {
        int exponential = retrys - 1;
        if (backOffMultiplier == 1) {
          delay = retryDelay;
        } else if (maxRetryDelay > 0) {
          if (exponential >= maxExponential) {
            delay = maxRetryDelay;
          } else {
            delay = Math.round(retryDelay * Math.pow(backOffMultiplier, exponential));
          }
        } else {
          delay = Math.round(retryDelay * Math.pow(backOffMultiplier, exponential));
        }
      } else {
        delay = retryDelay;
      }

      if (delay <= 0) {
        time = 0;
      } else if (maxRetryDelay > 0 && delay >= maxRetryDelay) {
        time = maxRetryDelay;
      } else {
        time = delay;
      }
    }

    if (expireTime > 0 && time >= 0 && (time + SystemClock.currentTimeMillis()) >= (startTime + expireTime)) {
      time = -1;
    }
    return time;
  }

  public long getDelay(final Instant expireTime, final int retryTimes) {
    long delay = 0;
    if (retryDelay > 0) {
      if (useExponentialBackOff) {
        int exponential = retryTimes - 1;
        delay = Math.round(retryDelay * Math.pow(backOffMultiplier, exponential));
      } else {
        delay = retryDelay;
      }
    }
    if (delay >= 0 && (SystemClock.currentTimeStamp().plusMillis(delay).isAfter(expireTime))) {
      delay = -1;
    }
    return delay;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RetryPolicy{");
    sb.append("maxRetry=").append(maxRetry);
    sb.append(", maxRetryDelay=").append(maxRetryDelay);
    sb.append(", retryDelay=").append(retryDelay);
    sb.append(", useExponentialBackOff=").append(useExponentialBackOff);
    sb.append(", backOffMultiplier=").append(backOffMultiplier);
    sb.append(", expireTime=").append(expireTime);
    sb.append('}');
    return sb.toString();
  }

  /**
   * RetryPolicy Builder.
   */
  public static final class Builder {
    // retry delay(ms)
    private int retryDelay = DEFAULT_RETRY_DELAY;
    // max retry delay(ms)
    private int maxRetryDelay = DEFAULT_MAX_RETRY_DELAY;
    // max retry(0=infinite)
    private int maxRetry = DEFAULT_MAX_RETRY;
    // expire time
    private int expireTime = DEFAULT_EXPIRE_TIME;
    // use exponential
    private boolean useExponentialBackOff = true;
    // exponential multiplier(>=1)
    private double backOffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

    public Builder maxRetry(int maxRetry) {
      this.maxRetry = maxRetry < 0 ? 0 : maxRetry;
      return this;
    }

    public Builder maxRetryDelay(int maxRetryDelay) {
      this.maxRetryDelay = maxRetryDelay < 0 ? 0 : maxRetryDelay;
      return this;
    }

    public Builder retryDelay(int retryDelay) {
      this.retryDelay = retryDelay < 0 ? 0 : retryDelay;
      return this;
    }

    public Builder useExponentialBackOff(boolean useExponentialBackOff) {
      this.useExponentialBackOff = useExponentialBackOff;
      return this;
    }

    public Builder backOffMultiplier(double backOffMultiplier) {
      this.backOffMultiplier = backOffMultiplier < 1 ? 1 : backOffMultiplier;
      return this;
    }

    public Builder expireTime(int expireTime) {
      this.expireTime = expireTime < 0 ? 0 : expireTime;
      return this;
    }

    public RetryPolicy build() {
      return new RetryPolicy(retryDelay, maxRetryDelay, maxRetry, expireTime, useExponentialBackOff, backOffMultiplier);
    }
  }
}
