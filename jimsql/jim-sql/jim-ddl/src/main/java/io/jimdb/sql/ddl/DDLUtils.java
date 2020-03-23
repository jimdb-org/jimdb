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
package io.jimdb.sql.ddl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.pb.Metapb.TableInfo;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "CE_CLASS_ENVY", "DLS_DEAD_LOCAL_STORE" })
public final class DDLUtils {
  protected static final int MAXLEN_PREFIX = 3072;
  protected static final int DEFAULT_REPLICA_NUM = 3;
  protected static final int DEFAULT_SEQ_REGION = 200;

  private static final Map<Basepb.DataType, Integer> DEFAULT_LENGTH_TYPE = new HashMap() {
    {
      put(Basepb.DataType.Year, 1);
      put(Basepb.DataType.Date, 3);
      put(Basepb.DataType.Time, 3);
      put(Basepb.DataType.DateTime, 8);
      put(Basepb.DataType.TimeStamp, 4);
      put(Basepb.DataType.TinyInt, 1);
      put(Basepb.DataType.SmallInt, 2);
      put(Basepb.DataType.MediumInt, 3);
      put(Basepb.DataType.Int, 4);
      put(Basepb.DataType.BigInt, 8);
      put(Basepb.DataType.Float, 4);
      put(Basepb.DataType.Double, 8);
    }
  };

  private static final Map<Integer, Integer> DEFAULT_LENGTH_FRACTION = new HashMap() {
    {
      put(0, 0);
      put(1, 1);
      put(2, 1);
      put(3, 2);
      put(4, 2);
      put(5, 3);
      put(6, 3);
    }
  };

  private DDLUtils() {
  }

  public static String trimName(String name) {
    name = StringUtils.removeStart(name, "'");
    name = StringUtils.removeEnd(name, "'");
    name = StringUtils.removeStart(name, "`");
    name = StringUtils.removeEnd(name, "`");
    return null == name ? null : name.trim();
  }

  public static Tuple2<String, String> splitTableName(SQLExpr expr) {
    return splitTableName(expr.toString());
  }

  public static Tuple2<String, String> splitTableName(String names) {
    names = trimName(names);
    int pos = names.lastIndexOf('.');
    String dbName = pos > 0 ? names.substring(0, pos) : "";
    String tableName = pos > 0 ? names.substring(pos + 1) : names;
    return Tuples.of(trimName(dbName), trimName(tableName));
  }

  public static IndexInfo buildIndexInfo(IndexInfo index, List<String> indexCols, Map<String, ColumnInfo> tableCols, List<IndexInfo> existIndexs) {
    List<String> indexNames = new ArrayList<>(existIndexs.size());
    List<List<Integer>> indexColumns = new ArrayList<>(existIndexs.size());
    for (IndexInfo indexInfo : existIndexs) {
      if (StringUtils.isNotBlank(indexInfo.getName())) {
        indexNames.add(indexInfo.getName().toLowerCase());
      }
      if (indexInfo.getColumnsCount() > 0) {
        indexColumns.add(indexInfo.getColumnsList());
      }
    }

    String name = index.getName();
    if (StringUtils.isBlank(name)) {
      if (index.getPrimary()) {
        name = Types.PRIMARY_KEY_NAME;
      }

      int i = 2;
      String colName = "";
      if (indexCols != null && indexCols.size() > 0) {
        colName = indexCols.get(0);
      }
      while (StringUtils.isBlank(name) || indexNames.contains(name.toLowerCase())) {
        name = String.format("%s_%d", colName, i++);
      }
    }
    if (indexNames.contains(name.toLowerCase())) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DUP_KEYNAME, name);
    }
    indexNames.add(name.toLowerCase());

    int i = 0;
    int sumLen = 0;
    List<Integer> colIDs = indexCols == Collections.EMPTY_LIST ? null : new ArrayList<>(indexCols.size());
    for (String indexCol : indexCols) {
      ColumnInfo columnInfo = tableCols.get(indexCol.toLowerCase());
      if (columnInfo == null) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_KEY_COLUMN_DOES_NOT_EXISTS, indexCol);
      }

      int len = verifyIndexColumnType(columnInfo);
      sumLen += len;
      if (sumLen > MAXLEN_PREFIX) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_TOO_LONG_KEY, String.valueOf(MAXLEN_PREFIX));
      }

      colIDs.add(columnInfo.getId());
      if (index.getPrimary()) {
        ColumnInfo.Builder builder = columnInfo.toBuilder();
        builder.setKeyFlag(columnInfo.getKeyFlag() | Types.FLAG_KEY_PRIMARY);
        tableCols.put(indexCol.toLowerCase(), builder.build());
      } else if (index.getUnique()) {
        if (i++ == 0) {
          ColumnInfo.Builder builder = columnInfo.toBuilder();
          builder.setKeyFlag(columnInfo.getKeyFlag() | (indexCols.size() > 1 ? Types.FLAG_KEY_MULTIPLE : Types.FLAG_KEY_UNIQUE));
          tableCols.put(indexCol.toLowerCase(), builder.build());
        }
      } else {
        if (i++ == 0) {
          ColumnInfo.Builder builder = columnInfo.toBuilder();
          builder.setKeyFlag(columnInfo.getKeyFlag() | Types.FLAG_KEY_MULTIPLE);
          tableCols.put(indexCol.toLowerCase(), builder.build());
        }
      }
    }

    for (List<Integer> indexColumn : indexColumns) {
      if (indexColumn.equals(colIDs)) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DUP_KEYNAME, Arrays.toString(indexCols.toArray()));
      }
    }

    IndexInfo.Builder builder = index.toBuilder();
    builder.setName(name)
            .addAllColumns(colIDs)
            .setState(Metapb.MetaState.Absent)
            .setCreateTime(SystemClock.currentTimeMillis());
    return builder.build();
  }

  private static int verifyIndexColumnType(ColumnInfo columnInfo) {
    SQLType sqlType = columnInfo.getSqlType();
    switch (sqlType.getType()) {
      case Varchar:
      case Char:
      case NChar:
        if (sqlType.getPrecision() == 0L) {
          throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_WRONG_KEY_COLUMN, columnInfo.getName());
        }
        break;

      case Json:
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_JSON_USED_AS_KEY, columnInfo.getName());
      case Binary:
      case VarBinary:
      case Text:
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_BLOB_KEY_WITHOUT_LENGTH, columnInfo.getName());
      default:
        break;
    }

    if (sqlType.getPrecision() > 0L) {
      if (sqlType.getType() == Basepb.DataType.Bit) {
        return ((int) sqlType.getPrecision() + 7) >> 3;
      }
      return (int) sqlType.getPrecision();
    }

    Integer len = DEFAULT_LENGTH_TYPE.get(sqlType.getType());
    if (len == null) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_META_UNKNOWN_TYPELEN, sqlType.getType().name());
    }

    int returnLen = len;
    if (sqlType.getScale() > 0 && Types.isFractionable(sqlType)) {
      Integer fracLen = DEFAULT_LENGTH_FRACTION.get(sqlType.getScale());
      if (fracLen == null) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_META_UNKNOWN_FRACTIONLEN, sqlType.getType().name(), String.valueOf(sqlType.getScale()));
      }
      returnLen += fracLen;
    }

    return returnLen;
  }

  private static Basepb.StoreType getTableStoreType(TableInfo tableInfo) {
    Basepb.StoreType rangeStoreType = Basepb.StoreType.Store_Warm;
    if (Basepb.StoreType.Store_Hot == tableInfo.getType()) {
      rangeStoreType = Basepb.StoreType.Store_Hot;
    }
    return rangeStoreType;
  }

  private static Basepb.StoreType getIndexStoreType(TableInfo tableInfo) {
    Basepb.StoreType rangeStoreType = Basepb.StoreType.Store_Hot;
    if (Basepb.StoreType.Store_Warm == tableInfo.getType()) {
      rangeStoreType = Basepb.StoreType.Store_Warm;
    }
    return rangeStoreType;
  }

  private static List<Basepb.KeySchema.ColumnInfo> getKeySchemaColInfos(List<Integer> columnList, Map<Integer, ColumnInfo> colMap) {
    List<Basepb.KeySchema.ColumnInfo> colInfos = new ArrayList<>(columnList.size());
    for (Integer colId : columnList) {
      ColumnInfo columnMeta = colMap.get(colId);
      Basepb.KeySchema.ColumnInfo colInfo = Basepb.KeySchema.ColumnInfo.newBuilder()
              .setId(columnMeta.getId())
              .setType(columnMeta.getSqlType().getType())
              .setUnsigned(columnMeta.getSqlType().getUnsigned()).build();
      colInfos.add(colInfo);
    }
    return colInfos;
  }

  //bigint singed                   -9223372036854775808 ~ 9223372036854775807
  //bigint unsigned                  0 ~ 18446744073709551615
  //bigint auto_increment            1 ~ 9223372036854775807
  //bigint unsigned auto_increment   1 ~ 18446744073709551615

  //int singed                      -2147483648 ~ 2147483647
  //int unsigned                     0 ~ 4294967295
  //int singed auto_increment        1 ~ 2147483647
  //int unsigned auto_increment      1 ~ 4294967295

  //only support unsigned auto_increment pre-split
  private static List<Basepb.Range> splitRangeByNum(TableInfo tableInfo, Map<Integer, ColumnInfo> colMap) {
    ColumnInfo splitColumn = colMap.get(tableInfo.getPrimarysList().get(0));
    SQLType type = splitColumn.getSqlType();
    List<ByteString> keys = getTableRangeKeys(tableInfo.getId());

    if (tableInfo.getPrimarysList().size() == 1 && Types.isIntegerType(type)) {
      Column[] columns = new Column[]{ new Column(null, splitColumn, 0) };
      Value[] values;
      BigInteger maxValue = ValueChecker.getUpperBound(type.getUnsigned(), type.getType());
      long docNum = maxValue.divide(BigInteger.valueOf(tableInfo.getSplitNum())).longValue();
      for (int j = 1; j < tableInfo.getSplitNum(); j++) {
        values = new Value[]{ UnsignedLongValue.getInstance(new BigInteger(Long.toUnsignedString(docNum * j))) };
        ByteString key = Codec.encodeKey(tableInfo.getId(), columns, values);
        keys.add(key);
      }
    }

    Basepb.KeySchema keySchema = Basepb.KeySchema.newBuilder().setUniqueIndex(true)
            .addAllKeyCols(getKeySchemaColInfos(tableInfo.getPrimarysList(), colMap)).build();
    return getRanges(tableInfo, keySchema, getTableStoreType(tableInfo), Basepb.RangeType.RNG_Data, keys);
  }

  private static List<ByteString> getTableRangeKeys(int tableId) {
    KvPair tableScope = Codec.encodeTableScope(tableId);
    List<ByteString> keys = new ArrayList<>(2);
    keys.add(tableScope.getKey());
    keys.add(tableScope.getValue());
    return keys;
  }

  private static List<Basepb.Range> splitRangeByKeys(TableInfo tableInfo, Map<Integer, ColumnInfo> colMap, List<ByteString> splitKeysList) {
    Basepb.KeySchema keySchema = Basepb.KeySchema.newBuilder().setUniqueIndex(true)
            .addAllKeyCols(getKeySchemaColInfos(tableInfo.getPrimarysList(), colMap)).build();

    Basepb.StoreType storeType = getTableStoreType(tableInfo);

    List<ByteString> keys = getTableRangeKeys(tableInfo.getId());

    ColumnInfo splitColumn = colMap.get(tableInfo.getPrimarysList().get(0));
    SQLType sqlType = splitColumn.getSqlType();

    //can split ? support num and varchar
    if (!Types.isNumberType(sqlType) && !Types.isVarCharType(sqlType)) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_META_UNSUPPORTED_PARTITION_TYPE, sqlType.getType().name());
    }

    Column[] columns = new Column[]{ new Column(null, splitColumn, 0) };
    Value[] values = new Value[1];
    splitKeysList.stream().forEach(splitKey -> {
      StringValue value = StringValue.getInstance(splitKey.toStringUtf8());
      values[0] = ValueConvertor.convertType(null, value, sqlType);
      ByteString key = Codec.encodeKey(tableInfo.getId(), columns, values);
      keys.add(key);
    });
    return getRanges(tableInfo, keySchema, storeType, Basepb.RangeType.RNG_Data, keys);
  }

  private static List<Basepb.Range> getRanges(Metapb.TableInfoOrBuilder tableInfo, Basepb.KeySchema keySchema, Basepb.StoreType storeType,
                                              Basepb.RangeType rangeType, List<ByteString> keys) {
    //sort keys
    Collections.sort(keys, (o1, o2) -> ByteUtil.compare(o1, o2));
    List<Basepb.Range> ranges = new ArrayList<>(keys.size() - 1);
    for (int i = 0; i < keys.size() - 1; i++) {
      Basepb.Range range = Basepb.Range.newBuilder()
              .setDbId(tableInfo.getDbId())
              .setTableId(tableInfo.getId())
              .setStartKey(keys.get(i))
              .setEndKey(keys.get(i + 1))
              .setKeySchema(keySchema)
              .setStoreType(storeType)
              .setRangeType(rangeType).build();
      ranges.add(range);
    }
    return ranges;
  }

  //create row data range, only support pre-split by first primary column
  public static List<Basepb.Range> buildTableRange(TableInfo tableInfo, Map<Integer, ColumnInfo> colMap) {
    List<ByteString> splitKeysList = tableInfo.getSplitKeysList().asByteStringList();
    if (splitKeysList.isEmpty()) {
      //split to SPLIT_RANGE_NUM or one range: If the user does not specify the splitKeys, the SPLIT_RANGE_NUM parameter may be used
      return splitRangeByNum(tableInfo, colMap);
    } else {
      //split by SplitKeys
      return splitRangeByKeys(tableInfo, colMap, splitKeysList);
    }
  }

  //create index data range
  public static List<Basepb.Range> buildIndexRange(TableInfo tableInfo, Map<Integer, ColumnInfo> colMap, List<IndexInfo> indicesList) {
    if (indicesList == null || indicesList.isEmpty() || (indicesList.size() == 1 && indicesList.get(0).getPrimary())) {
      return null;
    }

    Basepb.StoreType storeType = getIndexStoreType(tableInfo);
    List<Basepb.Range> indexRanges = new ArrayList<>();
    List<Basepb.KeySchema.ColumnInfo> pkColInfos = getKeySchemaColInfos(tableInfo.getPrimarysList(), colMap);
    indicesList.stream().filter(indexInfo -> !indexInfo.getPrimary()).forEach(indexInfo -> {
      Basepb.KeySchema keySchema = Basepb.KeySchema.newBuilder().setUniqueIndex(indexInfo.getUnique())
              .addAllKeyCols(getKeySchemaColInfos(indexInfo.getColumnsList(), colMap))
              .addAllExtraCols(pkColInfos).build();
      KvPair indexScope = Codec.encodeTableScope(indexInfo.getId());
      //todo index split range impl
      Basepb.Range range = Basepb.Range.newBuilder()
              .setDbId(tableInfo.getDbId())
              .setTableId(tableInfo.getId())
              .setIndexId(indexInfo.getId())
              .setStartKey(indexScope.getKey())
              .setEndKey(indexScope.getValue())
              .setKeySchema(keySchema)
              .setStoreType(storeType)
              .setRangeType(Basepb.RangeType.RNG_Index).build();
      indexRanges.add(range);
    });
    return indexRanges;
  }

  public static void main(String[] args) {
    List<Basepb.DataType> dataTypes = new ArrayList<>();
    dataTypes.add(Basepb.DataType.TinyInt);
    dataTypes.add(Basepb.DataType.SmallInt);
    dataTypes.add(Basepb.DataType.MediumInt);
    dataTypes.add(Basepb.DataType.Int);
    dataTypes.add(Basepb.DataType.BigInt);

    List<Boolean> unsignedList = new ArrayList<>();
    unsignedList.add(Boolean.FALSE);
    unsignedList.add(Boolean.TRUE);

    for (Basepb.DataType dataType : dataTypes) {
      for (Boolean unsigned : unsignedList) {
        BigInteger upperValue = ValueChecker.getUpperBound(unsigned, dataType);
        System.out.println(dataType + ", unsigned:" + unsigned + ",upper:" + upperValue);
        getScope(unsigned, dataType);
        System.out.println("==============");
      }
    }
  }

  private static void getScope(boolean unsigned, Basepb.DataType dataType) {
    BigInteger maxValue = ValueChecker.getUpperBound(unsigned, dataType);
    long docNum = maxValue.divide(BigInteger.valueOf(5)).longValue();
    System.out.println("step:" + docNum);
    Value[] values = new Value[1];
    for (int j = 1; j < 5; j++) {
      values[0] = UnsignedLongValue.getInstance(new BigInteger(Long.toUnsignedString(docNum * j)));
      System.out.println(values[0]);
    }
  }
}
