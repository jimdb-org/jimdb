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
package io.jimdb.common.utils.generator;

import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.jimdb.common.utils.lang.NetUtil;
import io.jimdb.common.utils.lang.RandomHolder;
import io.jimdb.common.utils.os.SystemClock;

/**
 * These are essentially flake ids but we use 6 (not 8) bytes for timestamp, and use 3 (not 2) bytes for sequence number.
 * For more information about flake ids, check out
 * https://archive.fo/2015.07.08-082503/http://www.boundary.com/blog/2012/01/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang/
 *
 * @version V1.0
 */
public final class UUIDGenerator {
  private static final byte[] ADDRESS = NetUtil.getMacAddress();

  // Use bottom 3 bytes for the sequence number.
  private static final AtomicLong SEQ = new AtomicLong(RandomHolder.INSTANCE.nextInt());

  private static final Lock TIME_LOCK = new ReentrantLock();

  private static long lastTimestamp;

  private UUIDGenerator() {
  }

  public static String next() {
    final long seqId = SEQ.incrementAndGet() & 0xffffff;
    long timestamp = SystemClock.currentTimeMillis();

    TIME_LOCK.lock();
    try {
      timestamp = Math.max(lastTimestamp, timestamp);
      if (seqId == 0L) {
        timestamp++;
      }
      lastTimestamp = timestamp;
    } finally {
      TIME_LOCK.unlock();
    }

    final byte[] uuidBytes = new byte[15];
    uuidBytes[0] = (byte) seqId;
    uuidBytes[1] = (byte) (seqId >>> 16);
    uuidBytes[2] = (byte) (timestamp >>> 16);
    uuidBytes[3] = (byte) (timestamp >>> 24);
    uuidBytes[4] = (byte) (timestamp >>> 32);
    uuidBytes[5] = (byte) (timestamp >>> 40);
    System.arraycopy(ADDRESS, 0, uuidBytes, 6, 6);
    uuidBytes[12] = (byte) (timestamp >>> 8);
    uuidBytes[13] = (byte) (seqId >>> 8);
    uuidBytes[14] = (byte) timestamp;

    return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes);
  }
}
