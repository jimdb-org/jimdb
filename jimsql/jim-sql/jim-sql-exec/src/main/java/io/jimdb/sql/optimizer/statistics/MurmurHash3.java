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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * MurmurHash3 is a wrapper of google murmur3 hash functions.
 */
public class MurmurHash3 {
  private static final int SEED = 0xabc45689;
  private static HashFunction hf = Hashing.murmur3_128(SEED);

  public static long hash(String s) {
    return hf.hashString(s, Charsets.UTF_8).asLong();
  }

  private static byte[] hash(byte[] bytes) {
    return hf.hashBytes(bytes).asBytes();
  }

  public static Tuple2<Long, Long> getHash128AsLongTuple(byte[] bytes) {
    byte[] hashBytes = hash(bytes);
    ByteBuffer bb = ByteBuffer.wrap(hashBytes).order(ByteOrder.LITTLE_ENDIAN);
    return Tuples.of(bb.getLong(0) >>> 1, bb.getLong(1) >>> 1);
  }

  public static byte[] getHash128(byte[] bytes) {
    return hash(bytes);
  }

  public static byte[] getHash64(byte[] bytes) {
    byte[] byte16 = hash(bytes);
    return Arrays.copyOfRange(byte16, 0, 8);
  }

  public static long getHash64AsLong(byte[] bytes) {
    return getHash128AsLongTuple(bytes).getT1();
  }
}

