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
package io.jimdb.test.engine.table;

import java.nio.ByteBuffer;

import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Encoding Tester.
 *
 * @version 1.0
 */
public class EncodingTest {
  private long tableId = 1000;
  private static final byte STORE_PREFIX = 0x01;

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  //100000 , 1141
  //10000000, 1093
  //1000000000, 7775
  @Test
  public void testLongToByte1() {
    long startTime = SystemClock.currentTimeMillis();
    for (int i = 0; i < 1000000000; i++) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
      buffer.put((byte) 0x01);
      buffer.putLong(1, tableId);
      byte[] bytes = buffer.array();
      System.out.println(ByteUtil.bytes2hex01(bytes));
    }
    System.out.println(SystemClock.currentTimeMillis() - startTime);
  }

  //100000,  131
  //10000000, 77
  //1000000000, 178
  @Test
  public void testLongToByte2() {
    long startTime = SystemClock.currentTimeMillis();
    for (int i = 0; i < 1000000000; i++) {
      byte[] bytes = new byte[9];
      bytes[0] = (byte) 0x01;
      bytes[1] = (byte) ((tableId >> 56) & 0xff);
      bytes[2] = (byte) ((tableId >> 48) & 0xff);
      bytes[3] = (byte) ((tableId >> 40) & 0xff);
      bytes[4] = (byte) ((tableId >> 32) & 0xff);
      bytes[5] = (byte) ((tableId >> 24) & 0xff);
      bytes[6] = (byte) ((tableId >> 16) & 0xff);
      bytes[7] = (byte) ((tableId >> 8) & 0xff);
      bytes[8] = (byte) (tableId & 0xff);
      System.out.println(ByteUtil.bytes2hex01(bytes));
    }
    System.out.println(SystemClock.currentTimeMillis() - startTime);
  }

  @Test
  public void testLongToByte3() {
    byte[] bytes = new byte[1];
    bytes[0] = (byte) -0x01;
    System.out.println(ByteUtil.bytes2Str(16, 1, bytes));
    System.out.println(ByteUtil.bytes2Str(2, 1, bytes));
    System.out.println(ByteUtil.bytes2binaryStr(bytes));
  }

  @Test
  public void testLongEncode() {
    ByteBuf heapBuf = Unpooled.buffer(8);
    heapBuf.setLong(0, 1);
    System.out.println(heapBuf.getLong(0));
  }
}

