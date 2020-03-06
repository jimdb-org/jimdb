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
package io.jimdb.common.utils.lang;

import java.math.BigInteger;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;

/**
 * Codec related utils
 *
 */
public final class ByteUtil {
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private ByteUtil() {
  }

  //default BigEndian

  //support change bytes to unsigned int at any byte length
  public static int bytesToUInt(byte[] buff) {
    int value = 0;
    for (int i = 0; i < buff.length; i++) {
      value |= (0xff & buff[i]) << ((buff.length - 1 - i) * 8);
    }
    return value;
//    return ((0xff & buff[0]) << 24)
//            | ((0xff & buff[1]) << 16)
//            | ((0xff & buff[2]) << 8)
//            | (0xff & buff[3]);
  }

  public static int bytesToInt(byte[] buff) {
    int value = 0;
    for (int i = 0; i < buff.length; i++) {
      value |= buff[i] << ((buff.length - 1 - i) * 8);
    }
    return value;
//    return (buff[0] << 24)
//            | (buff[1] << 16)
//            | (buff[2] << 8)
//            | buff[3];
  }

  public static long bytesToULong(byte[] buff) {
    long value = 0;
    for (int i = 0; i < buff.length; i++) {
      value |= (0xff & buff[i]) << ((buff.length - 1 - i) * 8);
    }
    return value;
//    return ((0xff & buff[0]) << 56)
//            | ((0xff & buff[1]) << 48)
//            | ((0xff & buff[2]) << 40)
//            | ((0xff & buff[3]) << 32)
//            | ((0xff & buff[4]) << 24)
//            | ((0xff & buff[5]) << 16)
//            | ((0xff & buff[6]) << 8)
//            | (0xff & buff[7]);
  }

  public static long bytesToLong(byte[] buff) {
    long value = 0;
    for (int i = 0; i < buff.length; i++) {
      value |= buff[i] << ((buff.length - 1 - i) * 8);
    }
    return value;
//    return (buff[0] << 56)
//            | (buff[1] << 48)
//            | (buff[2] << 40)
//            | (buff[3] << 32)
//            | (buff[4] << 24)
//            | (buff[5] << 16)
//            | (buff[6] << 8)
//            | buff[7];
  }

  public static byte[] intToBytes(int data) {
    byte[] buff = new byte[4];
    buff[0] = (byte) ((data >> 24) & 0xff);
    buff[1] = (byte) ((data >> 16) & 0xff);
    buff[2] = (byte) ((data >> 8) & 0xff);
    buff[3] = (byte) (data & 0xff);
    return buff;
  }

  public static byte[] longToBytes(long data) {
    byte[] buff = new byte[8];
    buff[0] = (byte) ((data >> 56) & 0xff);
    buff[1] = (byte) ((data >> 48) & 0xff);
    buff[2] = (byte) ((data >> 40) & 0xff);
    buff[3] = (byte) ((data >> 32) & 0xff);
    buff[4] = (byte) ((data >> 24) & 0xff);
    buff[5] = (byte) ((data >> 16) & 0xff);
    buff[6] = (byte) ((data >> 8) & 0xff);
    buff[7] = (byte) (data & 0xff);
    return buff;
  }

  public static byte[] typeToBytes(long data, int size) {
    byte[] buff = new byte[size];
    for (int i = 0; i < size; i++) {
      buff[i] = (byte) ((data >> (size - 1 - i) * 8) & 0xff);
    }
    return buff;
  }

  public static double bytesToDouble(byte[] buff) {
    long l = bytesToLong(buff);
    return Double.longBitsToDouble(l);
  }

  public static byte[] doubleToBytes(double data) {
    long bits = Double.doubleToLongBits(data);
    return longToBytes(bits);
  }

  public static int indexOf(byte[] bytes, byte value) {
    if (bytes == null) {
      return -1;
    } else {
      for (int i = 0; i < bytes.length; ++i) {
        if (value == bytes[i]) {
          return i;
        }
      }
      return -1;
    }
  }

  public static byte[] subBytes(byte[] bytes, int startIndexInclusive, int endIndexExclusive) {
    if (bytes == null) {
      return null;
    } else {
      if (startIndexInclusive < 0) {
        startIndexInclusive = 0;
      }

      if (endIndexExclusive > bytes.length) {
        endIndexExclusive = bytes.length;
      }

      int newSize = endIndexExclusive - startIndexInclusive;
      if (newSize <= 0) {
        return EMPTY_BYTE_ARRAY;
      } else {
        byte[] subBytes = new byte[newSize];
        System.arraycopy(bytes, startIndexInclusive, subBytes, 0, newSize);
        return subBytes;
      }
    }
  }

  /**
   * Convert bytes to specified radix str
   * @param radix TODO
   * @param signum TODO
   * @param bytes TODO
   * @return TODO
   */
  public static String bytes2Str(int radix, int signum, byte... bytes) {
    BigInteger bigInteger = new BigInteger(signum, bytes);
    return bigInteger.toString(radix);
  }

  /**
   * Convert bytes to string with Filling Trailing 0
   * @param bytes TODO
   * @return TODO
   */
  public static String bytes2StrWithFillTrailing0(byte... bytes) {
    StringBuilder builder = new StringBuilder();
    for (byte data : bytes) {
      builder.append(byte2StrWithFillTrailing0(data));
    }
    return builder.toString();
  }

  /**
   * convert byte to string with Filling Trailing 0
   * @param data TODO
   * @return TODO
   */
  public static String byte2StrWithFillTrailing0(byte data) {
    return Integer.toBinaryString((data & 0xFF) + 0x100).substring(1);
  }

  /**
   * Signum of the number (-1 for negative, 0 for zero, 1 for positive).
   */
  public static String bytes2hex01(byte... bytes) {
    return bytes2Str(16, 1, bytes);
  }

  /**
   * Signum of the number (-1 for negative, 0 for zero, 1 for positive).
   */
  public static String bytes2binaryStr(byte... bytes) {
    return bytes2Str(2, 1, bytes);
  }



  public static int compareSigned(byte[] data1, byte[] data2) {
    if (data1 == data2) {
      return 0;
    }
    int len = Math.min(data1.length, data2.length);
    for (int i = 0; i < len; i++) {
      byte b = data1[i];
      byte b2 = data2[i];
      if (b != b2) {
        return b > b2 ? 1 : -1;
      }
    }
    return Integer.signum(data1.length - data2.length);
  }

  public static int compareUnsigned(byte[] data1, byte[] data2) {
    if (data1 == data2) {
      return 0;
    }
    int len = Math.min(data1.length, data2.length);
    for (int i = 0; i < len; i++) {
      int b = data1[i] & 0xff;
      int b2 = data2[i] & 0xff;
      if (b != b2) {
        return b > b2 ? 1 : -1;
      }
    }
    return Integer.signum(data1.length - data2.length);
  }

  public static int getByteHash(byte[] value) {
    int len = value.length;
    int h = len;
    if (len < 50) {
      for (int i = 0; i < len; i++) {
        h = h * 31 + value[i];
      }
      return h;
    }

    final int step = len / 16;
    for (int i = 0; i < 4; i++) {
      h = h * 31 + value[i];
      h = h * 31 + value[--len];
    }
    for (int i = step + 4; i < len; i += step) {
      h = h * 31 + value[i];
    }
    return h;
  }

  public static int compare(ByteString left, ByteString right) {
    int minLen = Math.min(left.size(), right.size());
    for (int i = 0; i < minLen; i++) {
      int result = UnsignedBytes.compare(left.byteAt(i), right.byteAt(i));
      if (result != 0) {
        return result;
      }
    }
    return left.size() - right.size();
  }

  public static int compare(byte[] left, ByteString right) {
    int minLen = Math.min(left.length, right.size());
    for (int i = 0; i < minLen; i++) {
      int result = UnsignedBytes.compare(left[i], right.byteAt(i));
      if (result != 0) {
        return result;
      }
    }
    return left.length - right.size();
  }

  public static int compare(byte[] left, byte[] right) {
    int minLength = Math.min(left.length, right.length);

    for (int i = 0; i < minLength; ++i) {
      int result = UnsignedBytes.compare(left[i], right[i]);
      if (result != 0) {
        return result;
      }
    }

    return left.length - right.length;
  }
}
