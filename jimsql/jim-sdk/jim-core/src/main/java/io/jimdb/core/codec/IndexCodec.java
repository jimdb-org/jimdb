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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.values.Value;
import io.netty.buffer.ByteBuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
final class IndexCodec {
  private IndexCodec() {
  }

  static ByteString encodeKey(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
    if (index.isUnique()) {
      return IndexUniqueCodec.encodeKey(index, values, rowKey, offsetFlag);
    }
    return IndexNonUniqueCodec.encodeKey(index, values, rowKey, offsetFlag);
  }

  static KvPair encodeKV(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
    if (index.isUnique()) {
      return IndexUniqueCodec.encodeKV(index, values, rowKey, offsetFlag);
    }
    return IndexNonUniqueCodec.encodeKV(index, values, rowKey, offsetFlag);
  }

  static List<KvPair> encodeKeyRanges(int indexID, int colsSize, List<ValueRange> ranges, boolean isOptimizeKey) {
    List<KvPair> kvPairs = new ArrayList<>(ranges.size());
    for (ValueRange range : ranges) {
      kvPairs.add(encodeKeyRange(indexID, colsSize, range, isOptimizeKey));
    }
    return kvPairs;
  }

  private static KvPair encodeKeyRange(int indexID, int colsSize, ValueRange range, boolean isOptimizeKey) {
    ByteString startKey = encodeValues(indexID, range.getStarts());
    if (isOptimizeKey && colsSize == range.getStarts().size() && range.getStarts().equals(range.getEnds())) {
      return new KvPair(startKey, startKey, true);
    } else {
      ByteString endKey = encodeValues(indexID, range.getEnds());
      if (!range.isStartInclusive()) {
        startKey = Codec.nextKey(startKey);
      }
      if (range.isEndInclusive()) {
        endKey = Codec.nextKey(endKey);
      }
      return new KvPair(startKey, endKey, false);
    }
  }

  private static ByteString encodeValues(int indexID, List<Value> values) {
    ByteBuf buf = Codec.allocBuffer(128);
    Codec.encodePrefix(buf, indexID);
    for (Value value : values) {
      if (value.isMin() || value.isMax()) {
        break;
      }
      ValueCodec.encodeAscendingKey(buf, value);
    }
    return NettyByteString.wrap(buf);
  }

  /**
   * Format of Non-Unique Index Storage Structure:
   * +---------------------------------+-------------+
   * |              Key                |    Value    |
   * +---------------------------------+-------------+
   * | indexId + indexValue + PKValues |null(version)|
   * +---------------------------------+-------------+
   * version: proxy don't encode the parameter value
   *
   * @version V1.0
   */
  private static final class IndexNonUniqueCodec {
    private IndexNonUniqueCodec() {
    }

    static ByteString encodeKey(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
      ByteBuf buf = Codec.allocBuffer(128);
      int prefixLen = Codec.encodePrefix(buf, index.getId());

      Column[] columns = index.getColumns();
      for (int i = 0; i < columns.length; i++) {
        encodeAscendingKey(buf, values, offsetFlag, columns, i);
      }

      if (!rowKey.isEmpty()) {
        NettyByteString.writeValue(buf, rowKey.substring(prefixLen));
      }
      return NettyByteString.wrap(buf);
    }

    static KvPair encodeKV(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
      ByteString key = encodeKey(index, values, rowKey, offsetFlag);
      return new KvPair(key);
    }
  }

  /**
   * Format of Unique Index Storage Structure:
   * +-----------------------+----------------------+
   * |         Key           |        Value         |
   * +----------------------------------------------+
   * |  indexId + indexValue | PKValues | (version) |
   * +-----------------------+----------------------+
   * version: proxy don't encode the parameter value
   *
   * @version V1.0
   */
  private static final class IndexUniqueCodec {
    private IndexUniqueCodec() {
    }

    static ByteString encodeKey(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
      ByteBuf buf = Codec.allocBuffer(128);
      int prefixLen = Codec.encodePrefix(buf, index.getId());

      //Duplicate entry '1-2' for key 'id_v'
      boolean nullFlag = false;
      Column[] columns = index.getColumns();
      for (int i = 0; i < columns.length; i++) {
        Value colValue = encodeAscendingKey(buf, values, offsetFlag, columns, i);
        if (colValue == null || colValue.isNull()) {
          nullFlag = true;
        }
      }

      //append pkValues
      if (nullFlag && !rowKey.isEmpty()) {
        NettyByteString.writeValue(buf, rowKey.substring(prefixLen));
      }
      return NettyByteString.wrap(buf);
    }

    static KvPair encodeKV(Index index, Value[] values, ByteString rowKey, boolean offsetFlag) {
      ByteBuf buf = Codec.allocBuffer(128);
      int prefixLen = Codec.encodePrefix(buf, index.getId());

      //Duplicate entry '1-2' for key 'id_v'
      boolean nullFlag = false;
      Column[] columns = index.getColumns();
      for (int i = 0; i < columns.length; i++) {
        Value colValue = encodeAscendingKey(buf, values, offsetFlag, columns, i);
        if (colValue == null || colValue.isNull()) {
          nullFlag = true;
        }
      }

      //append pkValues

      //exist null in unique index values, append pkValues in key
      if (nullFlag) {
        NettyByteString.writeValue(buf, rowKey.substring(prefixLen));
        return new KvPair(NettyByteString.wrap(buf));
      }

      //not exist null in index values, append pkValues in value
      return new KvPair(NettyByteString.wrap(buf), rowKey.substring(prefixLen));
    }
  }

  private static Value encodeAscendingKey(ByteBuf buf, Value[] values, boolean offsetFlag, Column[] columns, int i) {
    int offset = offsetFlag ? columns[i].getOffset() : i;
    Value colValue = values[offset];
    ValueCodec.encodeAscendingKey(buf, colValue);
    return colValue;
  }
}
