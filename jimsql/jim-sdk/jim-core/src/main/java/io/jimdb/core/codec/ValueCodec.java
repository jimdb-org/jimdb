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
package io.jimdb.core.codec;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.MySqlDecimalConverter;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.YearValue;
import io.jimdb.pb.Basepb;
import io.netty.buffer.ByteBuf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Value Codec Util
 * <p>
 * see cockroach https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go
 *
 * @version 1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class ValueCodec {

  static final byte ESCAPE = 0x00;
  static final byte ESCAPED_TERM = 0x01;
  static final byte ESCAPED00 = (byte) 0xff;

  static final byte ENCODE_NULL = 0x00;
  static final byte ENCODE_NOT_NULL = 0x01;
  static final byte DOUBLE_NAN = ENCODE_NOT_NULL + 1;
  static final byte DOUBLE_NEG = DOUBLE_NAN + 1;
  static final byte DOUBLE_ZERO = DOUBLE_NEG + 1;
  static final byte DOUBLE_POS = DOUBLE_ZERO + 1;

  static final byte BYTES_MARKER = 0x12;
  static final byte BYTES_DESC_MARKER = BYTES_MARKER + 1;
  static final byte DATE_MARKER = BYTES_DESC_MARKER + 1;
  static final byte TIME_MARKER = DATE_MARKER + 1;
  static final byte DECIMAL_MARKER = TIME_MARKER + 1;

  static final int NO_COLUMNID = 0;

  //128
  static final byte INT_MIN = (byte) 0x80;
  // IntMax is the maximum int tag value.
  // 253
  static final byte INT_MAX = (byte) 0xfd;
  static final byte INT_MAX_WIDTH = 8;
  //136
  static final byte INT_ZERO = INT_MIN + INT_MAX_WIDTH;
  //109
  static final byte INT_SMALL = (byte) (INT_MAX - INT_ZERO - INT_MAX_WIDTH);

  /**
   *
   */
  public enum TagType {
    Unknown((byte) 0),
    Null((byte) 1),
    NotNull((byte) 2),
    Int((byte) 3),
    Double((byte) 4),
    Decimal((byte) 5),
    Bytes((byte) 6),
    BytesDesc((byte) 7),
    Date((byte) 8),
    Time((byte) 9),

    SentinelType((byte) 15);

    byte value;

    TagType(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return this.value;
    }

    public static TagType valueOf(byte value) {
      TagType e;
      switch (value) {
        case 0:
          e = Unknown;
          break;
        case 1:
          e = Null;
          break;
        case 2:
          e = NotNull;
          break;
        case 3:
          e = Int;
          break;
        case 4:
          e = Double;
          break;
        case 5:
          e = Decimal;
          break;
        case 6:
          e = Bytes;
          break;
        case 7:
          e = BytesDesc;
          break;
        case 8:
          e = Date;
          break;
        case 9:
          e = Time;
          break;
        case 15:
          e = SentinelType;
          break;
        default:
          throw new IllegalArgumentException("Unknown encode tag type value:" + value);
      }
      return e;
    }
  }

  private ValueCodec() {
  }

  public static void encodeNullAscending(ByteBuf buf) {
    buf.writeByte(ENCODE_NULL);
    return;
  }

  public static void encodeUvarintAscending(ByteBuf buff, long data) {
    if (data < 0) {
      buff.writeByte(INT_MAX).writeByte((byte) (data >>> 56)).writeByte((byte) (data >>> 48)).writeByte((byte) (data >>> 40))
              .writeByte((byte) (data >>> 32)).writeByte((byte) (data >>> 24))
              .writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= INT_SMALL) {
      buff.writeByte(INT_ZERO + (byte) data);
    } else if (data <= 0xff) {
      buff.writeByte(INT_MAX - 7).writeByte((byte) data);
    } else if (data <= 0xffff) {
      buff.writeByte(INT_MAX - 6).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= 0xffffff) {
      buff.writeByte(INT_MAX - 5).writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= 0xffffffffL) {
      buff.writeByte(INT_MAX - 4).writeByte((byte) (data >>> 24)).
              writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= 0xffffffffffL) {
      buff.writeByte(INT_MAX - 3).writeByte((byte) (data >>> 32)).writeByte((byte) (data >>> 24))
              .writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= 0xffffffffffffL) {
      buff.writeByte(INT_MAX - 2).writeByte((byte) (data >>> 40)).writeByte((byte) (data >>> 32))
              .writeByte((byte) (data >>> 24)).writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else if (data <= 0xffffffffffffffL) {
      buff.writeByte(INT_MAX - 1).writeByte((byte) (data >>> 48)).writeByte((byte) (data >>> 40))
              .writeByte((byte) (data >>> 32)).writeByte((byte) (data >>> 24))
              .writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    } else {
      buff.writeByte(INT_MAX).writeByte((byte) (data >>> 56)).writeByte((byte) (data >>> 48)).writeByte((byte) (data >>> 40))
              .writeByte((byte) (data >>> 32)).writeByte((byte) (data >>> 24))
              .writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
    }
  }

  public static void encodeVarintAscending(ByteBuf buf, long data) {
    if (data < 0) {
      if (data >= -0xff) {
        buf.writeByte(INT_MIN + 7).writeByte((byte) data);
      } else if (data >= -0xffff) {
        buf.writeByte(INT_MIN + 6).writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else if (data >= -0xffffff) {
        buf.writeByte(INT_MIN + 5).writeByte((byte) (data >> 16)).writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else if (data >= -0xffffffffL) {
        buf.writeByte(INT_MIN + 4).writeByte((byte) (data >> 24)).writeByte((byte) (data >> 16))
                .writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else if (data >= -0xffffffffffL) {
        buf.writeByte(INT_MIN + 3).writeByte((byte) (data >> 32)).writeByte((byte) (data >> 24)).writeByte((byte) (data >> 16))
                .writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else if (data >= -0xffffffffffffL) {
        buf.writeByte(INT_MIN + 2).writeByte((byte) (data >> 40)).writeByte((byte) (data >> 32)).writeByte((byte) (data >> 24))
                .writeByte((byte) (data >> 16)).writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else if (data >= -0xffffffffffffffL) {
        buf.writeByte(INT_MIN + 1).writeByte((byte) (data >> 48)).writeByte((byte) (data >> 40)).writeByte((byte) (data >> 32))
                .writeByte((byte) (data >> 24)).writeByte((byte) (data >> 16)).writeByte((byte) (data >> 8)).writeByte((byte) data);
      } else {
        buf.writeByte(INT_MIN).writeByte((byte) (data >> 56)).writeByte((byte) (data >> 48)).writeByte((byte) (data >> 40))
                .writeByte((byte) (data >> 32)).writeByte((byte) (data >> 24)).writeByte((int) (data >> 16))
                .writeByte((byte) (data >> 8)).writeByte((byte) data);
      }
    } else {
      encodeUvarintAscending(buf, data);
    }
  }

  public static void encodeDoubleAscending(ByteBuf buff, double value) {
    long intValue = Double.doubleToLongBits(value);
    if (Double.isNaN(value)) {
      buff.writeByte(DOUBLE_NAN);
      return;
    } else if (value == 0) {
      buff.writeByte(DOUBLE_ZERO);
      return;
    }
    if ((intValue & (1 << 63)) != 0) {
      intValue = ~intValue;
      buff.writeByte(DOUBLE_NEG);
    } else {
      buff.writeByte(DOUBLE_POS);
    }
    encodeUint64Ascending(buff, intValue);
  }

  //int64 -> long
  private static ByteBuf encodeUint64Ascending(ByteBuf buff, long data) {
    return buff.writeByte((byte) (data >>> 56)).writeByte((byte) (data >>> 48)).writeByte((byte) (data >>> 40)).writeByte((byte) (data >>> 32))
            .writeByte((byte) (data >>> 24)).writeByte((byte) (data >>> 16)).writeByte((byte) (data >>> 8)).writeByte((byte) data);
  }

  private static DecodeValue<LongValue> decodeUint64Ascending(ByteBuf buf) {
    if (buf.readableBytes() < 8) {
      throw new RuntimeException("insufficient bytes to decode uint64 int value");
    }

    DecodeValue<LongValue> decodeResult = new DecodeValue<>();
    decodeResult.setValue(LongValue.getInstance(buf.readLong()));
    return decodeResult;
  }

  public static void encodeDateAscending(ByteBuf buff, long time) {
    buff.writeByte(DATE_MARKER);
    encodeUvarintAscending(buff, time);
  }

  public static void encodeTimeAscending(ByteBuf buff, long time) {
    buff.writeByte(TIME_MARKER);
    encodeVarintAscending(buff, time);
  }

  public static void encodeBytesAscending(ByteBuf buff, byte[] data) {
    buff.writeByte(BYTES_MARKER);
    while (true) {
      int index = ByteUtil.indexOf(data, ESCAPE);
      if (index == -1) {
        break;
      }
      byte[] subBytes = ByteUtil.subBytes(data, 0, index);
      buff.writeBytes(subBytes);
      buff.writeByte(ESCAPE).writeByte(ESCAPED00);
      data = ByteUtil.subBytes(data, index + 1, data.length);
    }
    buff.writeBytes(data);
    buff.writeByte(ESCAPE).writeByte(ESCAPED_TERM);
  }

  public static void encodeDecimalAscending(ByteBuf buff, byte[] data) {
    buff.writeByte(DECIMAL_MARKER);
    buff.writeBytes(data);
  }

  public static ByteBuf encodeNonsortingUvarint(ByteBuf buf, long data) {
    if (data < 1 << 7) {
      return buf.writeByte((int) data);
    }
    if (data < 1L << 14) {
      return buf.writeByte(0x80 | (int) (data >> 7)).writeByte(0x7f & (int) data);
    }
    if (data < 1L << 21) {
      return buf.writeByte(0x80 | (int) (data >> 14)).writeByte(0x80 | (int) (data >> 7))
              .writeByte(0x7f & (int) data);
    }
    if (data < 1L << 28) {
      return buf.writeByte(0x80 | (int) (data >> 21)).writeByte(0x80 | (int) (data >> 14))
              .writeByte(0x80 | (int) (data >> 7)).writeByte(0x7f & (int) data);
    }
    if (data < 1L << 35) {
      return buf.writeByte(0x80 | (int) (data >> 28)).writeByte(0x80 | (int) (data >> 21))
              .writeByte(0x80 | (int) (data >> 14)).writeByte(0x80 | (int) (data >> 7))
              .writeByte(0x7f & (int) data);
    }
    if (data < 1L << 42) {
      return buf.writeByte(0x80 | (int) (data >> 35)).writeByte(0x80 | (int) (data >> 28))
              .writeByte(0x80 | (int) (data >> 21)).writeByte(0x80 | (int) (data >> 14))
              .writeByte(0x80 | (int) (data >> 7)).writeByte(0x7f & (int) data);
    }
    if (data < 1L << 49) {
      return buf.writeByte(0x80 | (int) (data >> 42)).writeByte(0x80 | (int) (data >> 35))
              .writeByte(0x80 | (int) (data >> 28)).writeByte(0x80 | (int) (data >> 21))
              .writeByte(0x80 | (int) (data >> 14)).writeByte(0x80 | (int) (data >> 7))
              .writeByte(0x7f & (int) data);
    }
    if (data < 1L << 56) {
      return buf.writeByte(0x80 | (int) (data >> 49)).writeByte(0x80 | (int) (data >> 42))
              .writeByte(0x80 | (int) (data >> 35)).writeByte(0x80 | (int) (data >> 28))
              .writeByte(0x80 | (int) (data >> 21)).writeByte(0x80 | (int) (data >> 14))
              .writeByte(0x80 | (int) (data >> 7)).writeByte(0x7f & (int) data);
    }

    return buf.writeByte(0x80 | (int) (data >> 56)).writeByte(0x80 | (int) (data >> 49))
            .writeByte(0x80 | (int) (data >> 42)).writeByte(0x80 | (int) (data >> 35))
            .writeByte(0x80 | (int) (data >> 28)).writeByte(0x80 | (int) (data >> 21))
            .writeByte(0x80 | (int) (data >> 14)).writeByte(0x80 | (int) (data >> 7))
            .writeByte(0x7f & (int) data);
  }

  public static DecodeValue<LongValue> decodeNonsortingUvarint(ByteBuf buf) {
    int i = 0;
    byte flag = -1;
    long value = 0;
    while (buf.isReadable()) {
      value = value << 7;
      byte b = buf.readByte();
      value += (long) (b & 0x7f);
      if ((b & flag) >= 0) {
        DecodeValue<LongValue> decodeResult = new DecodeValue<>();
        decodeResult.setIndex(i + 1);
        decodeResult.setValue(LongValue.getInstance(value));
        return decodeResult;
      }
      i++;
    }
    return null;
  }

  public static ByteBuf encodeNonsortingVarint(ByteBuf buf, long data) {
    writeSignedVarLong(buf, data);
    return buf;
  }

  public static DecodeValue<LongValue> decodeNonsortingVarint(ByteBuf buf) {
    DecodeValue<LongValue> decodeResult = readSignedVarLong(buf);
    if (decodeResult.getIndex() <= 0) {
      throw new RuntimeException(String.format("int64 varint decoding failed:%d", decodeResult.getIndex()));
    }
    return decodeResult;
  }

  public static ByteBuf encodeValueTag(ByteBuf buf, long colId, TagType type) {
    if (type.value > TagType.SentinelType.value) {
      buf = encodeNonsortingUvarint(buf, colId << 4 | (long) TagType.SentinelType.value);
      return encodeNonsortingUvarint(buf, (long) type.value);
    }
    if (colId == NO_COLUMNID) {
      return buf.writeByte(type.value);
    }
    return encodeNonsortingUvarint(buf, colId << 4 | (long) type.value);
  }

  public static DecodeTag decodeValueTag(ByteBuf buf) {
    if (buf == null || buf.readableBytes() < 0) {
      throw new RuntimeException("empty buf");
    }

    int readIndex = buf.readerIndex();
    DecodeValue<LongValue> decodeResult = decodeNonsortingUvarint(buf);
    long tag = decodeResult.getValue().getValue();
    int n = decodeResult.getIndex();
    TagType encodeType = TagType.valueOf((byte) (tag & 0xf));
    int typeOffset = n - 1;
    int dataOffset = n;
    if (encodeType.value == TagType.SentinelType.value) {
      decodeResult = decodeNonsortingUvarint(buf);
      encodeType = TagType.valueOf((byte) decodeResult.getValue().getValue());
      dataOffset += decodeResult.getIndex();
    }

    buf.readerIndex(readIndex);
    return new DecodeTag(typeOffset, dataOffset, encodeType);
  }

  //int64 -> long
  public static ByteBuf encodeIntValue(ByteBuf buf, long colId, long data) {
    buf = encodeValueTag(buf, colId, TagType.Int);
    return encodeNonsortingVarint(buf, data);
  }

  public static DecodeValue<LongValue> decodeIntValue(ByteBuf buf) {
    buf = decodeValueTypeAssert(buf, TagType.Int);
    return decodeNonsortingVarint(buf);
  }

  //flout64 -> double
  public static ByteBuf encodeDoubleValue(ByteBuf buf, long colId, double data) {
    buf = encodeValueTag(buf, colId, TagType.Double);
    return encodeUint64Ascending(buf, Double.doubleToLongBits(data));
  }

  public static DecodeValue<DoubleValue> decodeDoubleValue(ByteBuf buf) {
    buf = decodeValueTypeAssert(buf, TagType.Double);
    if (buf.readableBytes() < 8) {
      throw new RuntimeException(String.format("float64 value should be exactly 8 bytes: %d", buf.readableBytes()));
    }

    DecodeValue<LongValue> longDecodeResult = decodeUint64Ascending(buf);
    DecodeValue<DoubleValue> result = new DecodeValue<>();
    result.setValue(DoubleValue.getInstance(Double.longBitsToDouble(longDecodeResult.getValue().getValue())));
    return result;
  }

  public static ByteBuf encodeDecimalValue(ByteBuf buf, long colId, BigDecimal data, int precision, int scale) {
    buf = encodeValueTag(buf, colId, TagType.Decimal);
    byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(data, precision, scale);
    return buf.writeBytes(bytes);
  }

  public static DecodeValue<DecimalValue> decodeDecimalValue(ByteBuf buf) {
    decodeValueTypeAssert(buf, TagType.Decimal);
    int flagIndex = buf.readerIndex();
    byte precision = buf.readByte();
    byte scale = buf.readByte();
    int valueLength = MySqlDecimalConverter.getBytesLength(precision, scale);
    buf.readerIndex(flagIndex);
    byte[] value = new byte[valueLength + 2];
    buf.readBytes(value);
    BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(value);
    DecodeValue<DecimalValue> decodeResult = new DecodeValue<>();
    decodeResult.setValue(DecimalValue.getInstance(decimal, precision, scale));
    return decodeResult;
  }

  public static ByteBuf encodeBytesValue(ByteBuf buf, long colId, byte[] data) {
    buf = encodeValueTag(buf, colId, TagType.Bytes);
    buf = encodeNonsortingUvarint(buf, data.length);
    return buf.writeBytes(data);
  }

  public static ByteBuf encodeDateValue(ByteBuf buf, long colId, long time) {
    encodeValueTag(buf, colId, TagType.Date);
    encodeNonsortingUvarint(buf, time);
    return buf;
  }

  public static ByteBuf encodeTimeValue(ByteBuf buf, long colId, long time) {
    buf = encodeValueTag(buf, colId, TagType.Time);
    return encodeNonsortingVarint(buf, time);
  }

  public static DecodeValue<BinaryValue> decodeBytesValue(ByteBuf buf) {
    decodeValueTypeAssert(buf, TagType.Bytes);
    DecodeValue<LongValue> longDecodeResult = decodeNonsortingUvarint(buf);
    DecodeValue<BinaryValue> decodeResult = new DecodeValue<>();
    byte[] value = new byte[(int) longDecodeResult.getValue().getValue()];
    buf.readBytes(value);
    decodeResult.setValue(BinaryValue.getInstance(value));
    return decodeResult;
  }

  public static DecodeValue<DateValue> decodeDateValue(ByteBuf buf, Basepb.DataType dataType, int fsp) {
    decodeValueTypeAssert(buf, TagType.Date);
    DecodeValue<LongValue> longDecodeResult = decodeNonsortingUvarint(buf);
    DecodeValue<DateValue> decodeResult = new DecodeValue<>();
    decodeResult.setValue(DateValue.getInstance(longDecodeResult.getValue().getValue(), dataType, fsp));
    return decodeResult;
  }

  public static DecodeValue<TimeValue> decodeTimeValue(ByteBuf buf) {
    decodeValueTypeAssert(buf, TagType.Time);
    DecodeValue<LongValue> longDecodeResult = decodeNonsortingVarint(buf);
    DecodeValue<TimeValue> decodeResult = new DecodeValue<>();
    decodeResult.setValue(TimeValue.getInstance(longDecodeResult.getValue().getValue()));
    return decodeResult;
  }

  private static ByteBuf decodeValueTypeAssert(ByteBuf buf, TagType encodeType) {
    DecodeTag decodeValueTag = decodeValueTag(buf);
    if (decodeValueTag.getType().value != encodeType.value) {
      throw new RuntimeException(String.format("value type is not %d: %d", encodeType.value,
              decodeValueTag.getType().value));
    }

    buf.readerIndex(buf.readerIndex() + decodeValueTag.getDataOffset());
    return buf;
  }

  private static void writeSignedVarLong(ByteBuf buf, long value) {
    long ux = value << 1;
    if (value < 0) {
      ux = ~ux;
    }
    writeUnsignedVarLong(buf, ux);
  }

  private static void writeUnsignedVarLong(ByteBuf buf, long value) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf.writeByte(((int) value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf.writeByte((int) value & 0x7F);
  }

  private static DecodeValue<LongValue> readSignedVarLong(ByteBuf buf) {
    DecodeValue<LongValue> decodeResult = readUnsignedVarInt(buf);
    long ux = decodeResult.getValue().getValue();
    long x = ux >>> 1;
    if ((ux & 1) != 0L) {
      x = ~x;
    }
    decodeResult.setValue(LongValue.getInstance(x));
    return decodeResult;
  }

  private static DecodeValue<LongValue> readUnsignedVarInt(ByteBuf buf) {
    DecodeValue<LongValue> decodeResult = new DecodeValue<>();
    long x = 0;
    long s = 0;
    int i = 0;
    byte flag = -1;
    while (buf.isReadable()) {
      byte b = buf.readByte();
      if ((b & flag) >= 0) {
        if (i > 9 || i == 9 && b > 1) {
          decodeResult.setValue(LongValue.getInstance(0));
          decodeResult.setIndex(-(i + 1));
          return decodeResult;
        }

        decodeResult.setValue(LongValue.getInstance(x | ((long) b) << s));
        decodeResult.setIndex(i + 1);
        return decodeResult;
      }
      x |= (long) (b & 0x7f) << s;
      s += 7;
      i++;
    }
    return decodeResult;
  }

  public static void encodeAscendingKey(ByteBuf buf, Value value) {
    if (value == null || value.isNull()) {
      encodeNullAscending(buf);
      return;
    }
    switch (value.getType()) {
      case LONG:
        encodeVarintAscending(buf, ((LongValue) value).getValue());
        break;
      case UNSIGNEDLONG:
        encodeUvarintAscending(buf, ((UnsignedLongValue) value).getValue().longValue());
        break;
      case DOUBLE:
        encodeDoubleAscending(buf, ((DoubleValue) value).getValue());
        break;
      case DECIMAL:
        DecimalValue decimalValue = (DecimalValue) value;
        encodeDecimalAscending(buf, MySqlDecimalConverter.encodeToMySqlDecimal(decimalValue.getValue(),
                decimalValue.getPrecision(), decimalValue.getScale()));
        break;
      case STRING:
        encodeBytesAscending(buf, ((StringValue) value).getValue().getBytes(StandardCharsets.UTF_8));
        break;
      case BINARY:
        encodeBytesAscending(buf, ((BinaryValue) value).getValue());
        break;
      case DATE:
        long time = TimeUtil.encodeTimestampToUint64(((DateValue) value).getValue());
        ValueCodec.encodeDateAscending(buf, time);
        break;
      case TIME:
        encodeTimeAscending(buf, ((TimeValue) value).getValue());
        break;
      case YEAR:
        encodeVarintAscending(buf, ((YearValue) value).getValue());
        break;
      case JSON:
        encodeBytesAscending(buf, ((JsonValue) value).getValue().getBytes(StandardCharsets.UTF_8));
        break;
      default:
        break;
    }
  }

  public static void encodeValue(ByteBuf buf, Value value, int colId) {
    if (value == null || value.isNull()) {
      return;
    }
    switch (value.getType()) {
      case LONG:
        encodeIntValue(buf, colId, ((LongValue) value).getValue());
        break;
      case UNSIGNEDLONG:
        encodeIntValue(buf, colId, ((UnsignedLongValue) value).getValue().longValue());
        break;
      case DOUBLE:
        encodeDoubleValue(buf, colId, ((DoubleValue) value).getValue());
        break;
      case DECIMAL:
        DecimalValue decimalValue = (DecimalValue) value;
        encodeDecimalValue(buf, colId, decimalValue.getValue(), decimalValue.getPrecision(), decimalValue.getScale());
        break;
      case STRING:
        encodeBytesValue(buf, colId, ((StringValue) value).getValue().getBytes(StandardCharsets.UTF_8));
        break;
      case BINARY:
        encodeBytesValue(buf, colId, ((BinaryValue) value).getValue());
        break;
      case DATE:
        long time = TimeUtil.encodeTimestampToUint64(((DateValue) value).getValue());
        ValueCodec.encodeDateValue(buf, colId, time);
        break;
      case TIME:
        encodeTimeValue(buf, colId, ((TimeValue) value).getValue());
        break;
      case YEAR:
        encodeIntValue(buf, colId, ((YearValue) value).getValue());
        break;
      case JSON:
        encodeBytesValue(buf, colId, ((JsonValue) value).getValue().getBytes(StandardCharsets.UTF_8));
        break;
      default:
        break;
    }
  }
}
