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
package io.jimdb.codec;

import java.util.Arrays;

import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.codec.DecodeValue;
import io.jimdb.core.codec.ValueCodec;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.core.values.TimeValue;
import io.jimdb.pb.Basepb;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * ValueCodec Tester.
 *
 * @version 1.0
 */
public class ValueCodecTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testCodec() {
    long a = Double.doubleToLongBits(0.0001);
    System.out.println(a);

    byte[] b = new byte[]{ (byte) 0b11010110, (byte) 0b0, (byte) 0b0, (byte) 0b11010110 };
    int result = ByteUtil.bytesToUInt(b);
    System.out.println("unsigned result" + result);
    System.out.println("unsigned binary string" + Integer.toBinaryString(result));

    result = ByteUtil.bytesToInt(b);
    System.out.println("signed result" + result);
    System.out.println("binary string" + Integer.toBinaryString(result));
  }

  @Test
  public void testDateValueCodec() {
    long date = 1843848852151589232L;
    ByteBuf buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
    ValueCodec.encodeNonsortingUvarint(buf, date);
    //[-103, -53, -86, -40, -80, -32, -121, -78, 112]
//    byte[] bytes = new byte[buf.readableBytes()];
//    buf.readBytes(bytes);
//    System.out.println("date = " + Arrays.toString(bytes));
//
//    buf.resetReaderIndex();
    DecodeValue<LongValue> longValue = ValueCodec.decodeNonsortingUvarint(buf);
    Assert.assertEquals(date, longValue.getValue().getValue());
  }

  @Test
  public void testTimeValueCodec() {
    ByteBuf buf;
    DecodeValue<LongValue> longValue;
    Long[] times = new Long[]{ 41445111000L, -41445000000L };
    for (Long time : times) {
      buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
      ValueCodec.encodeNonsortingVarint(buf, time);
      //[-128, -115, -124, -27, -76, 2]
      //[-1, -116, -124, -27, -76, 2]
      longValue = ValueCodec.decodeNonsortingVarint(buf);
      Assert.assertEquals(time.longValue(), longValue.getValue().getValue());
    }
  }

  @Test
  public void testDateEncodeForDate() throws Exception {
    String[] strs = new String[]{ "2019-10-19", "2015-07-21 12:12:12.1212", "2015-07-22 12:12:12.1212" };
    for (String str : strs) {
      DateValue value = DateValue.getInstance(str, Basepb.DataType.Date, 0, null);
      System.out.println(value.getString());
      long num = TimeUtil.encodeTimestampToUint64(value.getValue());
      System.out.println(num);
      //1843848014431518720
      //1843850213454774272
      ByteBuf buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
      ValueCodec.encodeDateAscending(buf, num);
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //[20, -3, 25, -106, -86, 0, 0, 0, 0, 0]
      //[20, -3, 25, -106, -84, 0, 0, 0, 0, 0]
      //keep ascending order
      System.out.println("bytes= " + Arrays.toString(bytes));
      System.out.println(DateValue.getInstance(num, Basepb.DataType.DateTime, 6).getValue());
    }
  }

  @Test
  public void testDateEncodeForDateTime() throws Exception {
    String[] strs = new String[]{ "2019-10-29", "2015-07-21 12:12:12.1212", "2015-07-21 13:12:12.1212" };
    long colId = 2;
    for (String str : strs) {
      DateValue value = DateValue.getInstance(str, Basepb.DataType.DateTime, 6, null);
      System.out.println("old time :" + value.getString());
      long num = TimeUtil.encodeTimestampToUint64(value.getValue());
      System.out.println(num);
      //1843848852151589232
      //1843848920871065968
      ByteBuf buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
      ValueCodec.encodeDateAscending(buf, num);
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //[20, -3, 25, -106, -86, -61, 12, 1, -39, 112]
      //[20, -3, 25, -106, -86, -45, 12, 1, -39, 112]
      //keep ascending order
      System.out.println("ascending bytes= " + Arrays.toString(bytes));
      buf.clear();
      ValueCodec.encodeDateValue(buf, colId, num);
      bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //40, -32, -27, -114, -64, -31, -80, -43, -106, 51
      System.out.println("value bytes= " + Arrays.toString(bytes));

      buf.resetReaderIndex();
      DecodeValue<DateValue> decodeValue = ValueCodec.decodeDateValue(buf, Basepb.DataType.TimeStamp, 6);
      System.out.println("decode value " + decodeValue.getValue().getValue());
    }
  }

  @Test
  public void testDateEncodeForTimestamp() throws Exception {
    String[] strs = new String[]{ "2015-07-21 12:12:12.1212", "2015-07-21 13:12:12.1212" };
    for (String str : strs) {
      DateValue value = DateValue.getInstance(str, Basepb.DataType.TimeStamp, 6, null);
      long num = TimeUtil.encodeTimestampToUint64(value.getValue());
      System.out.println(num);
      //1843848852151589232
      //1843848920871065968
      ByteBuf buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
      ValueCodec.encodeDateAscending(buf, num);
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //[20, -3, 25, -106, -86, -61, 12, 1, -39, 112]
      //[20, -3, 25, -106, -86, -45, 12, 1, -39, 112]
      //keep ascending order
      System.out.println("bytes= " + Arrays.toString(bytes));
    }
  }

  @Test
  public void testTimeEncode() throws Exception {
    String[] strs = new String[]{ "-11:30:45", "11:30:45.111" };
    long colId = 2L;
    for (String str : strs) {
      TimeValue value = TimeValue.getInstance(str);
      long num = value.getValue();
      System.out.println(num);
      //-41445000000
      //41445111000
      ByteBuf buf = new UnpooledByteBufAllocator(false).heapBuffer(128);
      ValueCodec.encodeTimeAscending(buf, value.getValue());
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //[21, -125, -10, 89, -81, 124, -64]
      //[21, -6, 9, -90, 82, 52, -40]
      //keep ascending order
      System.out.println("bytes= " + Arrays.toString(bytes));
      buf.clear();
      ValueCodec.encodeTimeValue(buf, colId, value.getValue());
      bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      //41, -1, -116, -124, -27, -76, 2
      //41, -80, -45, -111, -27, -76, 2
      System.out.println("value bytes= " + Arrays.toString(bytes));

      buf.resetReaderIndex();
      DecodeValue<TimeValue> decodeValue = ValueCodec.decodeTimeValue(buf);
      System.out.println("decode value " + decodeValue.getValue().toString());
    }
  }
}
