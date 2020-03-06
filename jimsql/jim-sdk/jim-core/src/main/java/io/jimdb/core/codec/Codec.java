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

import java.math.BigInteger;
import java.util.List;
import java.util.TimeZone;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.pb.Txn;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class Codec {

  protected static final int PREFIX_LENGTH = 4;

  private static final ByteBufAllocator BYTE_BUF_ALLOCATOR = new UnpooledByteBufAllocator(false);

  private Codec() {
  }

  public static ByteBuf allocBuffer(int capacity) {
    return BYTE_BUF_ALLOCATOR.heapBuffer(capacity);
  }

  static int encodePrefix(ByteBuf buf, int tableID) {
    buf.writeByte((byte) ((tableID >> 24) & 0xff));
    buf.writeByte((byte) ((tableID >> 16) & 0xff));
    buf.writeByte((byte) ((tableID >> 8) & 0xff));
    buf.writeByte((byte) (tableID & 0xff));
    return PREFIX_LENGTH;
  }

  public static ByteString encodeTableKey(int tableID, ByteString pkValues) {
    ByteBuf buf = Codec.allocBuffer(4);
    Codec.encodePrefix(buf, tableID);
    buf.writeBytes(pkValues.toByteArray());
    return NettyByteString.wrap(buf);
  }

  public static KvPair encodeTableScope(int tableID) {
    return TableCodec.encodeTableScope(tableID);
  }

  public static ByteString encodeKey(int tableId, Column[] columns, Value[] values) {
    return TableCodec.encodeKey(tableId, columns, values);
  }

  public static ByteString encodeIndexKey(Index index, Value[] values, ByteString rowKey, boolean offset) {
    if (index.isPrimary()) {
      return rowKey;
    }

    return IndexCodec.encodeKey(index, values, rowKey, offset);
  }

  public static KvPair encodeIndexKV(Index index, Value[] values, ByteString rowKey) {
    if (index.isPrimary()) {
      return TableCodec.encodeKV(index, values, rowKey);
    }

    return IndexCodec.encodeKV(index, values, rowKey, true);
  }

  public static List<KvPair> encodeKeyRanges(Index index, List<ValueRange> ranges, boolean isOptimizeKey) {
    if (index.isPrimary()) {
      return TableCodec.encodeKeyRanges(index.getTable().getId(), index.getColumns().length, ranges, isOptimizeKey);
    }

    return IndexCodec.encodeKeyRanges(index.getId(), index.getColumns().length, ranges, isOptimizeKey);
  }

  public static ByteString nextKey(ByteString key) {
    byte b;
    for (int i = key.size() - 1; i >= 0; i--) {
      b = key.byteAt(i);
      if ((0xff & b) < 0xff) {
        ByteBuf buf = allocBuffer(i + 1);
        NettyByteString.writeValue(buf, key.substring(0, i));
        buf.writeByte(b + 1);
        return NettyByteString.wrap(buf);
      }
    }
    return null;
  }

  public static ValueAccessor decodeRow(ColumnExpr[] resultColumns, Txn.RowValueOrBuilder row, TimeZone zone) {
    int colLength = resultColumns.length;
    int valueLength = colLength + 1;
    Value[] values = new Value[valueLength];
    ByteBuf buf = NettyByteString.asByteBuf(row.getFields());

    for (int i = 0; i < colLength; i++) {
      values[i] = decodeValue(buf, resultColumns[i].getResultType(), i, zone);
    }
    values[colLength] = LongValue.getInstance(row.getVersion());
    return new RowValueAccessor(values);
  }

  public static ValueAccessor decodeRowWithOpt(ColumnExpr[] resultColumns, Txn.RowValueOrBuilder row,
                                               ByteString pkValues, TimeZone zone) {
    boolean opt = !pkValues.isEmpty();
    int colLength = resultColumns.length;
    int valueLength = opt ? colLength + 2 : colLength + 1;
    Value[] values = new Value[valueLength];
    ByteBuf buf = NettyByteString.asByteBuf(row.getFields());
    int i = 0;
    for (; i < colLength; i++) {
      values[i] = decodeValue(buf, resultColumns[i].getResultType(), i, zone);
    }
    if (opt) {
      values[i++] = BinaryValue.getInstance(pkValues.toByteArray());
    }
    values[i++] = LongValue.getInstance(row.getVersion());
    return new RowValueAccessor(values);
  }

  private static Value decodeValue(ByteBuf buf, SQLType sqlType, int offset, TimeZone zone) {
    DecodeValue decodeValue = decodeValueInternal(buf, sqlType, offset);
    Value value = decodeValue.getValue();
    if (value == null || value.isNull()) {
      return NullValue.getInstance();
    }
    if (DataType.BigInt == sqlType.getType() && sqlType.getUnsigned()) {
      long number = ((LongValue) value).getValue();
      return UnsignedLongValue.getInstance(new BigInteger(Long.toUnsignedString(number)));
    }
    if (DataType.TimeStamp == sqlType.getType()) {
      return DateValue.convertTimeZone((DateValue) value, TimeUtil.UTC_ZONE, zone);
    }
    return value;
  }

  private static DecodeValue<? extends Value> decodeValueInternal(ByteBuf buf, SQLType sqlType, int offset) {
    DecodeTag decodeValueTag = ValueCodec.decodeValueTag(buf);
    ValueCodec.TagType type = decodeValueTag.getType();
    if (type == ValueCodec.TagType.Null) {
      buf.readerIndex(buf.readerIndex() + 1);
      DecodeValue<NullValue> result = new DecodeValue();
      result.setValue(NullValue.getInstance());
      return result;
    }

    DataType dataType = sqlType.getType();
    switch (dataType) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
      case Year:
        return ValueCodec.decodeIntValue(buf);
      case Float:
      case Double:
        return ValueCodec.decodeDoubleValue(buf);
      case Decimal:
        return ValueCodec.decodeDecimalValue(buf);
      case Varchar:
      case Char:
      case NChar:
      case Text:
      case Binary:
      case VarBinary:
      case MediumBlob:
      case TinyBlob:
      case LongBlob:
      case Blob:
        return ValueCodec.decodeBytesValue(buf);
      case Date:
      case DateTime:
      case TimeStamp:
        return ValueCodec.decodeDateValue(buf, dataType, sqlType.getScale());
      case Time:
        return ValueCodec.decodeTimeValue(buf);
      default:
        throw new RuntimeException(String.format("unsupported type(%s) when encoding column(%d)", dataType, offset));
    }
  }
}
