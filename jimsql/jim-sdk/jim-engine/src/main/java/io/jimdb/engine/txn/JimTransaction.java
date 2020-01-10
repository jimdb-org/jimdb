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
package io.jimdb.engine.txn;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DistSender;
import io.jimdb.engine.table.alloc.IDAllocator;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.meta.RouterManager;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Txn;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.core.values.ValueConvertor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import reactor.core.publisher.Flux;

/**
 * Define implement for transaction.
 *
 * @version V1.0
 */
public class JimTransaction implements Transaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(JimTransaction.class);

  private static final ValueAccessor[][] ROW_VALUE_ACCESSORS_ARRAY_EMPTY = { QueryExecResult.EMPTY_ROW };

  private static final SelectFunc SELECT_FUNC = TxnHandler::select;
  private static final SelectFlowKeysFunc SELECT_KEYS_FLOW_FUNC = TxnHandler::selectFlowForKeys;
  private static final SelectFlowRangeFunc SELECT_RANGE_FLOW_FUNC = TxnHandler::selectFlowForRange;
  private static final SelectFlowFunc SELECT_FLOW_STREAM_FUNC = TxnHandler::selectFlowStream;

  private TxnConfig config;
  private volatile Txn.TxnStatus status = Txn.TxnStatus.TXN_INIT;

  private final Session session;

  private RouterManager routerManager;
  private DistSender distSender;
  private IDAllocator idAllocator;

  public JimTransaction(Session session, RouterManager routerManager, DistSender sender, IDAllocator allocator) {
    this.session = session;
    this.routerManager = routerManager;
    this.distSender = sender;
    this.idAllocator = allocator;
    int ttl = 0;
    if (session != null) {
      ttl = Integer.parseInt(session.getVarContext().getGlobalVariable("innodb_lock_wait_timeout"));
    }
    this.config = new TxnConfig(ttl);
  }

  @Override
  public Flux<ExecResult> insert(Table table, Column[] insertCols, List<Expression[]> rows,
                                 Assignment[] duplicate, boolean hasRefColumn) throws JimException {
    if (rows == null || rows.isEmpty()) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int rowSize = rows.size();
    Column[] tblColumns = table.getWritableColumns();
    RowValueAccessor rowAccessor = new RowValueAccessor(null);
    List<Value[]> rowValues = new ArrayList<>(rowSize);
    for (int i = 0; i < rowSize; i++) {
      rowValues.add(this.evalRow(tblColumns, insertCols, rows.get(i), rowAccessor, hasRefColumn, i));
    }

    Index[] indexes = table.getWritableIndices();
    Column[] pkColumns = table.getPrimary();

    long lastInsertId = batchFillAutoCol(table.getCatalog().getId(), table.getId(), pkColumns[0], rowValues);

    rowValues.stream().forEach(newData -> this.insertRow(table, indexes, pkColumns, newData));

    return Flux.just(new DMLExecResult(rowSize, lastInsertId));
  }

  private long batchFillAutoCol(int dbId, int tableId, Column pkColumn, List<Value[]> rowValues) {
    if (!pkColumn.isAutoIncr()) {
      return DMLExecResult.EMPTY_INSERT_ID;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("start to get auto increment doc ids");
    }
    int offset = pkColumn.getOffset();
    List<Integer> indexList = new ArrayList<>(0);
    for (int i = 0; i < rowValues.size(); i++) {
      Value autoValue = rowValues.get(i)[offset];
      if (autoValue == null || autoValue.isNull()) {
        indexList.add(i);
      } else if (!(autoValue instanceof UnsignedLongValue)) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "auto_increment except unsigned long");
      } else if (((UnsignedLongValue) autoValue).getValue().compareTo(new BigInteger("0")) <= 0) {
        indexList.add(i);
      }
    }
    if (indexList.isEmpty()) {
      return DMLExecResult.EMPTY_INSERT_ID;
    }
    long lastInsertId = DMLExecResult.EMPTY_INSERT_ID;
    List<UnsignedLongValue> autoIncrIds = idAllocator.alloc(dbId, tableId, indexList.size());
    for (int i = 0; i < indexList.size(); i++) {
      if (i == 0) {
        lastInsertId = autoIncrIds.get(i).getValue().longValue();
      }
      rowValues.get(i)[offset] = autoIncrIds.get(i);
    }
    return lastInsertId;
  }

  private void insertRow(Table table, Index[] indexes, Column[] pkColumns, Value[] colValues) {
    //storage key
    ByteString recordKey = Codec.encodeKey(table.getId(), pkColumns, colValues);

    Arrays.stream(indexes).forEach(index -> {
      KvPair idxKvPair = Codec.encodeIndexKV(index, colValues, recordKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[insert]txn {} encode index [{}]", this.config.getTxnId(), idxKvPair);
      }
      this.config.addIntent(cvtKVtoIntent(idxKvPair, Txn.OpType.INSERT, true, 0), table);
    });
  }

  private Value[] evalRow(Column[] tblCols, Column[] insertCols, Expression[] insertColExprs, RowValueAccessor rowAccessor,
                          boolean hasRefColumn, int rowIdx) {
    int colSize = tblCols.length;
    Value[] result = new Value[colSize];
    boolean[] hasVal = new boolean[colSize];
    if (hasRefColumn) {
      this.setRefColumnValue(tblCols, result, hasVal);
    }
    rowAccessor.setValues(result);

    Expression colExpr;
    Value colValue = null;
    int i = 0;
    try {
      for (; i < insertColExprs.length; i++) {
        colExpr = insertColExprs[i];
        colValue = colExpr.exec(rowAccessor);
        if (colValue == null || colValue.isNull()) {
          continue;
        }
        colValue = convertValue(session, insertCols[i], colValue);
        ValueChecker.checkValue(insertCols[i].getType(), colValue);
        int offset = insertCols[i].getOffset();
        result[offset] = colValue;
        hasVal[offset] = true;
      }
    } catch (JimException ex) {
      this.handleException(insertCols[i].getName(), colValue, rowIdx, ex);
    }

    this.fillRow(tblCols, result, hasVal);
    return result;
  }

  public static Value convertValue(Session session, Column column, Value value) {
    Metapb.SQLType sqlType = column.getType();
    Value result = ValueConvertor.convertType(session, value, sqlType);
    if (result == null || result.isNull()) {
      return NullValue.getInstance();
    }

    if (sqlType.getType() == Basepb.DataType.Varchar || sqlType.getType() == Basepb.DataType.Char
            || sqlType.getType() == Basepb.DataType.NChar || sqlType.getType() == Basepb.DataType.Text) {
      String str = result.getString();
      str = StringUtil.trimTail(str);
      result = StringValue.getInstance(str);
    }
    return result;
  }

  private void setRefColumnValue(Column[] tblCols, Value[] row, boolean[] hasVal) {
    Value defVal;
    for (Column column : tblCols) {
      defVal = column.getDefaultValue();
      row[column.getOffset()] = defVal;
      if (defVal == null || defVal.isNull()) {
        hasVal[column.getOffset()] = false;
        return;
      }
      if (!column.isAutoIncr()) {
        hasVal[column.getOffset()] = true;
      }
    }
  }

  private void fillRow(Column[] tblCols, Value[] row, boolean[] hasVal) {
    int offset;
    for (Column column : tblCols) {
      if (column.isAutoIncr()) {
        continue;
      }
      offset = column.getOffset();
      row[offset] = this.fillColumn(column, row[offset], hasVal[offset]);
      row[offset] = this.handleColumnNull(column.getType(), column.getName(), row[offset]);
    }
  }

  private Value fillColumn(Column column, Value value, boolean hasValue) {
    if (hasValue) {
      return value;
    }
    value = column.getDefaultValue();
    SQLType sqlType = column.getType();
    boolean notNull = sqlType.getNotNull();
    if (notNull && (value == null || value.isNull())) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NO_DEFAULT_FOR_FIELD, column.getName());
    }

    if (sqlType.getOnInit()) {
      value = DateValue.getNow(sqlType.getType(), sqlType.getScale(), session.getStmtContext().getLocalTimeZone());
    }

    return value;
  }

  private Value handleColumnNull(SQLType type, String colName, Value value) {
    if (type.getNotNull() && (value == null || value.isNull())) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_NULL_ERROR, colName);
    }
    return value;
  }

  private void handleException(String columnName, Value value, int rowIdx, JimException ex) {
    if (ex.getCode() == ErrorCode.ER_SYSTEM_VALUE_TOO_LONG) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_DATA_TOO_LONG, ex, columnName, String.valueOf(rowIdx + 1));
    }

    if (ex.getCode() == ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW
            || ex.getCode() == ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_UP
            || ex.getCode() == ErrorCode.ER_SYSTEM_VALUE_OVER_FLOW_DOWN) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WARN_DATA_OUT_OF_RANGE, ex, columnName, String.valueOf(rowIdx + 1));
    }

    if (ex.getCode() == ErrorCode.ER_SYSTEM_VALUE_TRUNCATED) {
      String str = value.getString();
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, ex, str, columnName, String.valueOf(rowIdx + 1));
    }
    throw ex;
  }

  @Override
  public Flux<ExecResult> update(Table table, Assignment[] assignments, QueryResult rows) throws JimException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int resultRowLength = rows.size();
    Index[] indexes = table.getWritableIndices();
    StoreCtx storeCtx = getStoreCtx(table);
    boolean[] assignFlag = new boolean[table.getWritableColumns().length];
    for (Assignment assignment : assignments) {
      assignFlag[assignment.getColumn().getOffset()] = true;
    }

    Flux<Long>[] versionFlux = new Flux[1];
    rows.forEach(row -> {
      LongValue recordVersion = (LongValue) row.get(row.size() - 1);
      Value[] oldData = accessValues(row, rows.getColumns());
      Value[] newData = computeNewValues(session, oldData, row, assignments);
      versionFlux[0] = updateRow(table, indexes, storeCtx, versionFlux[0], assignFlag, recordVersion, oldData, newData);
    });

    return evalExecResultFlux(resultRowLength, versionFlux[0]);
  }

  private Flux<Long> updateRow(Table table, Index[] indexes, StoreCtx storeCtx, Flux<Long> versionFlux,
                               boolean[] assignFlag, LongValue recordVersion, Value[] oldData, Value[] newData) {
    ScanVerFunc scanVerFunc = (ctx, key, idx) -> txnScanForVersion(ctx, key, idx);

    ByteString recordKey = Codec.encodeKey(table.getId(), table.getPrimary(), newData);
    for (Index index : indexes) {
      if (!index.isPrimary() && !checkChange(index, assignFlag)) {
        continue;
      }
      KvPair newIdxKvPair = Codec.encodeIndexKV(index, newData, recordKey);
      if (index.isPrimary()) {
        this.config.addIntent(cvtKVtoIntent(newIdxKvPair, Txn.OpType.INSERT, false, recordVersion.getValue()), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[update]txn {} encode new record:[{}], version：{}", this.config.getTxnId(),
                  newIdxKvPair, recordVersion.getValue());
        }
        continue;
      }

      KvPair oldIdxKvPair = Codec.encodeIndexKV(index, oldData, recordKey);

      Flux<Long> txnScanFlux = scanVerFunc.apply(storeCtx, oldIdxKvPair.getKey(), index)
              .onErrorResume(TxnHandler.getErrHandler(storeCtx, scanVerFunc, oldIdxKvPair.getKey(), index)).map(v -> {
//                if (v == 0) {
//                  return v;
//                }
                //value must be equal, so compare with key
                if (oldIdxKvPair.getKey().equals(newIdxKvPair.getKey())) {
                  this.config.addIntent(cvtKVtoIntent(oldIdxKvPair, Txn.OpType.INSERT, false, v), table);
                } else {
                  this.config.addIntent(cvtKVtoIntent(oldIdxKvPair, Txn.OpType.DELETE, false, v), table);
                  this.config.addIntent(cvtKVtoIntent(newIdxKvPair, Txn.OpType.INSERT, true, 0), table);
                }
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[update]txn {} encode old index:[{}], version:{} ; new index:[{}]", this.config.getTxnId(),
                          oldIdxKvPair, v, newIdxKvPair);
                }
                return v;
              });

      if (versionFlux == null) {
        versionFlux = txnScanFlux;
      } else {
        versionFlux = versionFlux.flatMap(v -> txnScanFlux);
      }
    }
    return versionFlux;
  }

  @Override
  public Flux<ExecResult> delete(Table table, QueryResult rows) throws JimException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int resultRowLength = rows.size();
    StoreCtx storeCtx = getStoreCtx(table);
    ColumnExpr[] selectColumns = rows.getColumns();
    Index[] indexes = table.getDeletableIndices();

    Map<Integer, Integer> colMap = getMapping(selectColumns);

    Flux<Long>[] versionFlux = new Flux[1];
    rows.forEach(row -> {
      LongValue recordVersion = (LongValue) row.get(selectColumns.length);
      Value[] partOldValue = accessValues(row, table.getWritableColumns(), colMap);
      versionFlux[0] = deleteRow(table, storeCtx, indexes, versionFlux[0], recordVersion, partOldValue);
    });

    return evalExecResultFlux(resultRowLength, versionFlux[0]);
  }

  private Flux<ExecResult> evalExecResultFlux(int resultRowLength, Flux<Long> versionFlux) {
    ExecResult result = new DMLExecResult(resultRowLength);
    if (versionFlux != null) {
      return versionFlux.map(v -> result);
    }
    return Flux.create(sink -> sink.next(result));
  }

  private Flux<Long> deleteRow(Table table, StoreCtx storeCtx, Index[] indexes, Flux<Long> versionFlux,
                               LongValue recordVersion, Value[] partOldValue) {
    ByteString recordKey = Codec.encodeKey(table.getId(), table.getPrimary(), partOldValue);
    ScanVerFunc scanVerFunc = (ctx, key, idx) -> txnScanForVersion(ctx, key, idx);

    for (Index index : indexes) {
      ByteString oldIdxKey = Codec.encodeIndexKey(index, partOldValue, recordKey, true);
      KvPair oldKvPair = new KvPair(oldIdxKey);

      if (index.isPrimary()) {
        this.config.addIntent(cvtKVtoIntent(oldKvPair, Txn.OpType.DELETE, false, recordVersion.getValue()), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[delete]txn {} encode old index:[{}], version:{} ", this.config.getTxnId(),
                  oldIdxKey, recordVersion.getValue());
        }
        continue;
      }

      //scan version
      Flux<Long> txnScanFlux =
              scanVerFunc.apply(storeCtx, oldIdxKey, index).onErrorResume(TxnHandler.getErrHandler(storeCtx,
                      scanVerFunc, oldIdxKey, index)).map(v -> onErr(table, oldIdxKey, oldKvPair, v));

      if (versionFlux == null) {
        versionFlux = txnScanFlux;
      } else {
        versionFlux = versionFlux.flatMap(v -> txnScanFlux);
      }
    }
    return versionFlux;
  }

  private Long onErr(Table table, ByteString oldIdxKey, KvPair oldKvPair, Long v) {
//    if (v == 0) {
//      return v;
//    }
    this.config.addIntent(cvtKVtoIntent(oldKvPair, Txn.OpType.DELETE, false, v), table);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[delete]txn {} encode old index:[{}], version:[{}]", this.config.getTxnId(),
              oldIdxKey, v);
    }
    return v;
  }

  private Map<Integer, Integer> getMapping(ColumnExpr[] columnExprs) {
    int colLength = columnExprs.length;
    Map<Integer, Integer> mapping = new HashMap<>(colLength);
    for (int i = 0; i < colLength; i++) {
      ColumnExpr expr = columnExprs[i];
      mapping.put(expr.getId(), i);
    }
    return mapping;
  }

  @Override
  public Flux<ExecResult> get(List<Index> indexes, List<Value[]> values, ColumnExpr[] resultColumns) {
    if (indexes == null || indexes.isEmpty()) {
      return Flux.create(sink -> sink.next(new QueryExecResult(resultColumns, TxnHandler.ROW_VALUE_ACCESSORS_EMPTY)));
    }

    Instant timeout = this.session.getStmtContext().getTimeout();
    ScanPKFunc scanPkFunc = this::txnScanForPk;

    Flux<ValueAccessor[]> result = null;
    for (int i = 0; i < indexes.size(); i++) {
      Flux<ValueAccessor[]> subResult = null;
      Index index = indexes.get(i);
      Value[] indexVals = values.get(i);
      Table table = index.getTable();
      StoreCtx storeCtx = StoreCtx.buildCtx(table, timeout, this.routerManager, this.distSender);

      Txn.SelectRequest.Builder reqBuilder =
              Txn.SelectRequest.newBuilder().addAllFieldList(getSelectFields(resultColumns));
      if (index.isPrimary()) {
        reqBuilder.setKey(Codec.encodeKey(table.getId(), index.getColumns(), indexVals));
        subResult =
                SELECT_FUNC.apply(storeCtx, reqBuilder, resultColumns).onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FUNC, reqBuilder, resultColumns));
      } else if (index.isUnique()) {
        //indexValue cannot exist null
        for (int j = 0; j < indexVals.length; j++) {
          Value value = indexVals[j];
          if (value == null || value.isNull()) {
            subResult = Flux.just(ROW_VALUE_ACCESSORS_ARRAY_EMPTY);
            break;
          }
        }
        if (subResult != null) {
          continue;
        }
        ByteString idxKey = Codec.encodeIndexKey(index, values.get(i), ByteString.EMPTY, false);
        Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
                .setStartKey(idxKey)
                .setEndKey(Codec.nextKey(idxKey))
                .setOnlyOne(true);
        Flux<List<ByteString>> searchPkFlux =
                scanPkFunc.apply(storeCtx, request, index).onErrorResume(TxnHandler.getErrHandler(storeCtx,
                        scanPkFunc, request, index));
        subResult = searchPkFlux.flatMap(pks -> {
          if (pks == null || pks.isEmpty()) {
            return Flux.just(ROW_VALUE_ACCESSORS_ARRAY_EMPTY);
          }
          if (pks.size() > 1) {
            throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_UNKNOWN_IN_INDEX, index.getName());
          }
          reqBuilder.setKey(Codec.encodeTableKey(table.getId(), pks.get(0)));
          return SELECT_FUNC.apply(storeCtx, reqBuilder, resultColumns).onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FUNC, reqBuilder, resultColumns));
        });
      } else {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "NOT supported with non-unique "
                + "condition index.");
      }
      if (result == null) {
        result = subResult;
      } else {
        result = result.zipWith(subResult, this::getValueAccessors);
      }
    }
    return result.map(rows -> new QueryExecResult(resultColumns, rows));
  }

  private ValueAccessor[] getValueAccessors(ValueAccessor[] f1, ValueAccessor[] f2) {
    if (f1 == null || f1.length == 0) {
      return f2;
    }
    if (f2 == null || f2.length == 0) {
      return f1;
    }
    ValueAccessor[] merge = new ValueAccessor[f1.length + f2.length];
    System.arraycopy(f1, 0, merge, 0, f1.length);
    System.arraycopy(f2, 0, merge, f1.length, f2.length);
    return merge;
  }

  @Override
  public Flux<ExecResult> select(Table table, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList) throws JimException {
    if (processors == null || processors.isEmpty()) {
      return changeRowsToValueAccessor(resultColumns, null);
    }

    StoreCtx storeCtx = getStoreCtx(table);
    KvPair kvPair = Codec.encodeTableScope(table.getId());
    Flux<ValueAccessor[]> valuesFlux = getSelectFluxForRange(processors, resultColumns, outputOffsetList, storeCtx,
            kvPair, true);
    return changeRowsToValueAccessor(resultColumns, valuesFlux);
  }

  @Override
  public Flux<ExecResult> select(Index index, List<Processorpb.Processor.Builder> processors,
                                 ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList, List<ValueRange> ranges) throws JimException {
    if (processors == null || processors.isEmpty()) {
      return changeRowsToValueAccessor(resultColumns, null);
    }

    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges);
    Table table = index.getTable();
    StoreCtx storeCtx = getStoreCtx(table);

    List<ByteString> keys = new ArrayList<>(1);
    List<KvPair> rangePairs = new ArrayList<>(1);
    //table reader
    if (index.isPrimary()) {
      for (int i = 0; i < ranges.size(); i++) {
        ValueRange valueRange = ranges.get(i);
        KvPair kvPair = kvPairs.get(i);
        if (valueRange.getStarts().equals(valueRange.getEnds())) {
          keys.add(kvPair.getKey());
        } else {
          rangePairs.add(kvPair);
        }
      }
    } else {
      rangePairs = kvPairs;
    }

    Flux<ValueAccessor[]> valuesFlux = getSelectFluxForTableKeys(processors, resultColumns, outputOffsetList, storeCtx, keys);
    if (!rangePairs.isEmpty()) {
      for (KvPair kvPair : rangePairs) {
        Flux<ValueAccessor[]> valuesEachRangeFlux = getSelectFluxForRange(processors, resultColumns, outputOffsetList,
                storeCtx, kvPair, index.isPrimary());
        if (valuesFlux == null) {
          valuesFlux = valuesEachRangeFlux;
        } else {
          valuesFlux = valuesFlux.zipWith(valuesEachRangeFlux, (f1, f2) -> getValueAccessors(f1, f2));
        }
      }
    }

    return changeRowsToValueAccessor(resultColumns, valuesFlux);
  }

  private StoreCtx getStoreCtx(Table table) {
    Instant timeout = null;
    if (session != null) {
      timeout = this.session.getStmtContext().getTimeout();
    }
    return StoreCtx.buildCtx(table, timeout, this.routerManager, this.distSender);
  }

  private Flux<ValueAccessor[]> getSelectFluxForRange(List<Processorpb.Processor.Builder> processors,
                                                      ColumnExpr[] resultColumns, List<Integer> outputOffsetList,
                                                      StoreCtx storeCtx, KvPair kvPair, boolean isPrimary) {
    Txn.SelectFlowRequest.Builder builder;
    if (isPrimary) {
      builder = getTableReaderForRange(processors, outputOffsetList, kvPair);
    } else {
      builder = getIndexReaderForRange(processors, outputOffsetList, kvPair);
    }
    return SELECT_RANGE_FLOW_FUNC.apply(storeCtx, builder, resultColumns, kvPair)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_RANGE_FLOW_FUNC, builder, resultColumns, kvPair));
  }

  private Flux<ValueAccessor[]> getSelectFluxForTableKeys(List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                                                          List<Integer> outputOffsetList, StoreCtx storeCtx, List<ByteString> keys) {
    if (keys.isEmpty()) {
      return null;
    }
    Txn.SelectFlowRequest.Builder builder = getTableReaderReqForKeys(processors, outputOffsetList, keys);
    return SELECT_KEYS_FLOW_FUNC.apply(storeCtx, builder, resultColumns, keys)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_KEYS_FLOW_FUNC, builder, resultColumns, keys));
  }

  private Txn.SelectFlowRequest.Builder getIndexReaderForRange(List<Processorpb.Processor.Builder> processors,
                                                               List<Integer> outputOffsetList, KvPair kvPair) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.INDEX_READ_TYPE) {
        Processorpb.IndexRead.Builder indexRead = Processorpb.IndexRead.newBuilder(processor.getIndexRead())
                .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                .setRange(Processorpb.KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setIndexRead(indexRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(true);
    return builder;
  }

  private Txn.SelectFlowRequest.Builder getTableReaderForRange(List<Processorpb.Processor.Builder> processors,
                                                               List<Integer> outputOffsetList,
                                                               KvPair kvPair) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.TABLE_READ_TYPE) {
        Processorpb.TableRead.Builder tableRead = Processorpb.TableRead.newBuilder(processor.getTableRead())
                .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                .setRange(Processorpb.KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(true);
    return builder;
  }

  private Txn.SelectFlowRequest.Builder getTableReaderReqForKeys(List<Processorpb.Processor.Builder> processors,
                                                                 List<Integer> outputOffsetList, List<ByteString> keys) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.TABLE_READ_TYPE) {
        Processorpb.TableRead.Builder tableRead = Processorpb.TableRead.newBuilder(processor.getTableRead())
                .setType(Processorpb.KeyType.PRIMARY_KEY_TYPE).addAllPkKeys(keys);
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(true);
    return builder;
  }

  private Flux<ExecResult> changeRowsToValueAccessor(ColumnExpr[] resultColumns, Flux<ValueAccessor[]> valuesFlux) {
    if (valuesFlux == null) {
      return Flux.create(sink -> sink.next(new QueryExecResult(resultColumns, TxnHandler.ROW_VALUE_ACCESSORS_EMPTY)));
    }
    return valuesFlux.map(rows -> {
      if (rows == null || rows.length == 0) {
        return new QueryExecResult(resultColumns, TxnHandler.ROW_VALUE_ACCESSORS_EMPTY);
      }
      return new QueryExecResult(resultColumns, rows);
    });
  }

  @Override
  public Flux<ValueAccessor[]> selectByStream(Table table, Txn.SelectFlowRequest.Builder builder,
                                              ColumnExpr[] resultColumns) throws JimException {

    StoreCtx storeCtx = getStoreCtx(table);
    Processorpb.KeyRange keyRange = builder.getProcessors(0).getTableRead().getRange();
    KvPair kvPair = new KvPair(keyRange.getStartKey(), keyRange.getEndKey());
    return SELECT_FLOW_STREAM_FUNC.apply(storeCtx, builder, resultColumns, kvPair)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FLOW_STREAM_FUNC, builder, resultColumns, kvPair));
  }

  @Override
  public void addIndex(Index index, ColumnExpr[] columns, ValueAccessor[] rows) {
    Table table = index.getTable();
    Map<Integer, Integer> colMap = getMapping(columns);
    Map<ByteString, Boolean> indexDulpMap = new HashMap<>();
    StoreCtx storeCtx = getStoreCtx(table);

    Arrays.stream(rows).forEach(row -> {
      Value[] partOldValue = accessValues(row, table.getWritableColumns(), colMap);
      ByteString rowKey = Codec.encodeKey(table.getId(), table.getPrimary(), partOldValue);
      KvPair rowKvPair = new KvPair(rowKey);

      KvPair idxKvPair = Codec.encodeIndexKV(index, partOldValue, rowKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[addIndex]txn {} encode index [{}]", this.config.getTxnId(), idxKvPair);
      }
      Boolean flag = indexDulpMap.get(idxKvPair.getKey());
      if (flag != null && flag.booleanValue()) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY,
                Arrays.toString(idxKvPair.getKey().toByteArray()), "INDEX");
      } else {
        indexDulpMap.put(idxKvPair.getKey(), Boolean.TRUE);
      }

      //exist index key?
      Boolean exist = this.txnScanForExist(storeCtx, idxKvPair.getKey(), index).blockFirst();
      if (exist != null && exist.booleanValue()) {
        return;
      }
      LongValue recordVersion = (LongValue) row.get(columns.length);
      //row add lock,
      this.config.addIntent(cvtKVtoIntent(rowKvPair, Txn.OpType.LOCK, false, recordVersion.getValue()), table);
      //index add lock
      this.config.addIntent(cvtKVtoIntent(idxKvPair, Txn.OpType.INSERT, true, 0), table);
    });
  }

  @Override
  public Flux<ExecResult> commit() throws JimException {
    if (!isPending()) {
      changeTxnStatus(Txn.TxnStatus.COMMITTED);
      close();
      return Flux.just(AckExecResult.getInstance());
    }

    changeTxnStatus(Txn.TxnStatus.COMMITTED);
    TxnConfig curConfig = this.close();

    StoreCtx storeCtx = getStoreCtx(curConfig.getTable());

    if (curConfig.isLocal()) {
      return Txn1PLHandler.commit(curConfig, storeCtx).map(r -> {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("1PL: txn {} commit success", curConfig.getTxnId());
        }
        return r;
      });
    }

    curConfig.sortIntents();

    return Txn2PLHandler.commit(curConfig, storeCtx)
            .map(r -> {
              if (LOGGER.isInfoEnabled()) {
                LOGGER.info("2PL: txn {} commit success", curConfig.getTxnId());
              }
              this.sendTask(new CommitSuccessTask(curConfig, storeCtx));
              return r;
            }).doOnError(e -> {
              //rollback
              LOGGER.error("2PL: txn {} commit error, start to rollback, err:", curConfig.getTxnId(), e);
              Txn2PLHandler.rollback(curConfig, storeCtx).subscribe(new TxnCallbackSubscriber(curConfig.getTxnId()));
            });
  }

  @Override
  public Flux<ExecResult> rollback() throws JimException {
    if (!isPending()) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_ERROR_DURING_ROLLBACK, "transaction has no DML statement"
              + " or has been rolled back");
    }

    changeTxnStatus(Txn.TxnStatus.ABORTED);

    TxnConfig curConfig = this.close();

    StoreCtx storeCtx = getStoreCtx(curConfig.getTable());

    if (curConfig.isLocal()) {
      return Txn1PLHandler.rollback(curConfig, storeCtx);
    }

    curConfig.sortIntents();
    return Txn2PLHandler.rollback(curConfig, storeCtx);
  }

  @Override
  public boolean isPending() {
    return !this.config.emptyIntents();
  }

  private Value[] accessValues(ValueAccessor accessor, Column[] columns, Map<Integer, Integer> mapping) {
    int colLength = columns.length;
    Value[] values = new Value[colLength];
    for (int i = 0; i < colLength; i++) {
      Column column = columns[i];
      Integer location = mapping.get(column.getId());
      if (location == null) {
        continue;
      }
      Value value = accessor.get(location);
      values[column.getOffset()] = value;
    }
    return values;
  }

  private Value[] accessValues(ValueAccessor accessor, ColumnExpr[] columns) {
    int colLength = columns.length;
    Value[] values = new Value[colLength];
    for (int i = 0; i < colLength; i++) {
      ColumnExpr expr = columns[i];
      Value value = expr.exec(accessor);
      values[expr.getOffset()] = value;
    }
    return values;
  }

  private Value[] computeNewValues(Session session, Value[] oldValues, ValueAccessor values, Assignment[] assignments) {
    //todo valid data scope
    Value[] newValues = cloneValue(oldValues);
    // An auto-updated column remains unchanged if all other columns are set to their current values.
    boolean changed = false;
    Value autoUpdateValue = null;
    int autoUpdateOffset = 0;
    Value newValue = null;
    for (int i = 0; i < assignments.length; i++) {
      Assignment assignment = assignments[i];
      ColumnExpr columnExpr = assignment.getColumn();
      try {
        Value value = assignment.getExpression().exec(values);
        SQLType sqlType = columnExpr.getResultType();
        newValue = ValueConvertor.convertType(session, value, sqlType);
        //todo verify that auto-updated column are extracted
        if (newValue == null && sqlType.getOnUpdate()
                && (sqlType.getType() == Basepb.DataType.TimeStamp || sqlType.getType() == Basepb.DataType.DateTime)) {
          autoUpdateValue = DateValue.getNow(sqlType.getType(), sqlType.getScale(), session.getStmtContext().getLocalTimeZone());
          autoUpdateOffset = columnExpr.getOffset();
          continue;
        }
        handleColumnNull(sqlType, columnExpr.getOriCol(), newValue);
        if (value.compareTo(null, newValue) != 0) {
          changed = true;
        }
        ValueChecker.checkValue(sqlType, newValue);
        newValues[columnExpr.getOffset()] = newValue;
        values.set(columnExpr.getOffset(), newValue);
      } catch (JimException ex) {
        this.handleException(columnExpr.getOriCol(), newValue, i, ex);
      }
    }
    if (changed) {
      newValues[autoUpdateOffset] = autoUpdateValue;
      values.set(autoUpdateOffset, autoUpdateValue);
    }

    return newValues;
  }

  private Value[] cloneValue(Value[] values) {
    Value[] newValues = new Value[values.length];
    System.arraycopy(values, 0, newValues, 0, values.length);
    return newValues;
  }

  private boolean checkChange(Index index, boolean[] assignFlag) {
    boolean changeFLag = false;
    for (Column column : index.getColumns()) {
      if (assignFlag[column.getOffset()]) {
        changeFLag = true;
        break;
      }
    }
    return changeFLag;
  }

  private List<Txn.SelectField> getSelectFields(ColumnExpr[] resultColumns) {
    List<Txn.SelectField> selectFields = new ArrayList<>(resultColumns.length);
    //select fields
    for (ColumnExpr column : resultColumns) {
      Txn.SelectField.Builder field = Txn.SelectField.newBuilder()
              .setColumn(convertColumnBuilder(column))
              .setTyp(Txn.SelectField.Type.Column);
      //todo consider isCount, setAggreFunc("count")
      selectFields.add(field.build());
    }
    return selectFields;
  }

  private Exprpb.ColumnInfo.Builder convertColumnBuilder(ColumnExpr expr) {
    Exprpb.ColumnInfo.Builder builder = Exprpb.ColumnInfo.newBuilder()
            .setId(expr.getId().intValue())
            .setTyp(expr.getResultType().getType())
            .setUnsigned(expr.getResultType().getUnsigned());

    if (expr.getReorgValue() != null) {
      builder.setReorgValue(expr.getReorgValue());
    }
    return builder;
    //todo
  }

  private void changeTxnStatus(Txn.TxnStatus newStatus) throws JimException {
    if (newStatus != Txn.TxnStatus.TXN_INIT
            && newStatus != Txn.TxnStatus.COMMITTED
            && newStatus != Txn.TxnStatus.ABORTED) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_STATE_INVALID, newStatus.name());
    }
    Txn.TxnStatus oldStatus = this.status;

    this.status = newStatus;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("change transaction status from {} to {}", oldStatus, newStatus);
    }
  }

  public Flux<Long> txnScanForVersion(StoreCtx storeCtx, ByteString startKey, Index index) {
    ByteString endKey = Codec.nextKey(startKey);
    Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
            .setStartKey(startKey).setEndKey(endKey).setOnlyOne(true);

    return TxnHandler.txnScan(storeCtx, request)
            .map(kvs -> {
              if (kvs.size() != 1) {
                throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
              }
              if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                LOGGER.error("txnScanForVersion: ctx {}  scan result {}", storeCtx.getCxtId(), kvs);
//                return 0L;
                throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
              }
              Long version = kvs.get(0).getVersion();
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("txn scan index {} kv, version:{}", index.getName(), version);
              }
              if (version == 0) {
                LOGGER.error("txn scan index {} kv, version:{}", index.getName(), version);
              }
              return version;
            });
  }

  public Flux<Boolean> txnScanForExist(StoreCtx storeCtx, ByteString startKey, Index index) {
    ByteString endKey = Codec.nextKey(startKey);
    Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
            .setStartKey(startKey).setEndKey(endKey).setOnlyOne(true);

    return TxnHandler.txnScan(storeCtx, request)
            .map(kvs -> {
              if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                return Boolean.FALSE;
              }
              if (kvs.size() > 1) {
                throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
              }
              return Boolean.TRUE;
            });
  }

  public Flux<List<ByteString>> txnScanForPk(StoreCtx storeCtx, Txn.ScanRequest.Builder request, Index index) {
    //todo config
    request.setMaxCount(10000);
    return TxnHandler.txnScan(storeCtx, request).map(kvs -> {
      if (kvs.isEmpty()) {
        return Collections.EMPTY_LIST;
      }

      List<ByteString> pkValuesList = new ArrayList<>(kvs.size());
      for (Txn.KeyValue kv : kvs) {
        if (kv == null || kv.getValue() == null) {
          continue;
        }

        try {
          ByteString pkValues = kv.getValue();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("txn scan index {} kv, pkValues:{}", index.getName(), pkValues);
          }
          pkValuesList.add(pkValues);
        } catch (Throwable e) {
          LOGGER.error("decode record key from [{}] err:{}", kv, e);
          throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_RPC_REQUEST_CODEC, e);
        }
      }
      return pkValuesList;
    });
  }

  private Txn.TxnIntent cvtKVtoIntent(KvPair kvPair, Txn.OpType opType, boolean check, long version) {
    Txn.TxnIntent.Builder txnIntent = Txn.TxnIntent.newBuilder()
            .setTyp(opType).setKey(kvPair.getKey());
    if (kvPair.getValue() != null) {
      txnIntent.setValue(kvPair.getValue());
    }
    txnIntent.setCheckUnique(check).setExpectedVer(version);
    return txnIntent.build();
  }

  //release resource
  private TxnConfig close() {
    TxnConfig oldConfig = this.config;
    this.config = new TxnConfig(oldConfig.getLockTTl());
    this.changeTxnStatus(Txn.TxnStatus.TXN_INIT);
    return oldConfig;
  }

  private void sendTask(TxnAsyncTask runnable) {
    if (runnable != null) {
      this.distSender.asyncTask(runnable);
    }
  }
}
