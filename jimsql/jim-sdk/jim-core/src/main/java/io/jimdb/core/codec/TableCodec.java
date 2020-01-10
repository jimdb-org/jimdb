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
package io.jimdb.core.codec;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.values.Value;
import io.netty.buffer.ByteBuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Format of Record Data Storage Structure:
 * +--------------------------------------------+
 * |          Key       |           Value       |
 * +--------------------------------------------+
 * | tableId + PKValues | NoPKValues | (version)|
 * +--------------------------------------------+
 * <p>
 * key: tableId(4个byte) + PKValues(长度不确定),  PKValues:  PK1_Column_Value + ... + PKN_Column_Value
 * <p>
 * version: proxy don't encode the parameter value
 *
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
final class TableCodec {

  private TableCodec() {
  }

  static ByteString encodeKey(int tableID, Column[] keys, Value[] values) {
    ByteBuf buf = Codec.allocBuffer(32);
    Codec.encodePrefix(buf, tableID);
    for (Column key : keys) {
      int offset = key.getOffset();
      Value value = values[offset];
      if (value == null || value.isNull()) {
        //primary key cannot be null
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_BAD_NULL_ERROR, key.getName());
      }
      ValueCodec.encodeAscendingKey(buf, value);
    }
    return NettyByteString.wrap(buf);
  }

  static ByteString encodeValue(Column[] columns, Value[] values) {
    ByteBuf buf = Codec.allocBuffer(50);
    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      if (col.isPrimary()) {
        continue;
      }
      if (i > values.length) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_WRONG_VALUE_COUNT);
      }
      ValueCodec.encodeValue(buf, values[i], col.getId());
    }
    return NettyByteString.wrap(buf);
  }

  static KvPair encodeKV(Index index, Value[] values, ByteString rowKey) {
    Column[] columns = index.getTable().getWritableColumns();
    return new KvPair(rowKey, encodeValue(columns, values));
  }

  static KvPair encodeTableScope(int tableID) {
    ByteBuf buf = Codec.allocBuffer(4);
    Codec.encodePrefix(buf, tableID);
    ByteString startKey = NettyByteString.wrap(buf);
    ByteString endKey = Codec.nextKey(startKey);
    return new KvPair(startKey, endKey);
  }

  static List<KvPair> encodeKeyRanges(int tableID, List<ValueRange> ranges) {
    List<KvPair> kvPairs = new ArrayList<>(ranges.size());
    for (ValueRange range : ranges) {
      ByteString lowKey = encodeKeyRange(tableID, range.getStarts());
      ByteString highKey = encodeKeyRange(tableID, range.getEnds());
      if (!range.isStartInclusive()) {
        lowKey = Codec.nextKey(lowKey);
      }
      if (range.isEndInclusive()) {
        highKey = Codec.nextKey(highKey);
      }
      kvPairs.add(new KvPair(lowKey, highKey));
    }
    return kvPairs;
  }

  private static ByteString encodeKeyRange(int tableID, List<Value> values) {
    ByteBuf buf = Codec.allocBuffer(32);
    Codec.encodePrefix(buf, tableID);
    for (Value value : values) {
      if (value.isMin() || value.isMax()) {
        break;
      }
      ValueCodec.encodeAscendingKey(buf, value);
    }
    return NettyByteString.wrap(buf);
  }
}
