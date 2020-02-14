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

import static java.util.Collections.EMPTY_LIST;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.sender.DistSender;
import io.jimdb.engine.table.alloc.IDAllocator;
import io.jimdb.meta.RouterManager;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.pb.Processorpb.IndexRead;
import io.jimdb.pb.Processorpb.KeyRange;
import io.jimdb.pb.Processorpb.KeyType;
import io.jimdb.pb.Processorpb.Limit;
import io.jimdb.pb.Processorpb.Processor;
import io.jimdb.pb.Processorpb.ProcessorType;
import io.jimdb.pb.Processorpb.TableRead;
import io.jimdb.pb.Txn.KeyValue;
import io.jimdb.pb.Txn.OpType;
import io.jimdb.pb.Txn.ScanRequest;
import io.jimdb.pb.Txn.SelectField;
import io.jimdb.pb.Txn.SelectFlowRequest;
import io.jimdb.pb.Txn.SelectRequest;
import io.jimdb.pb.Txn.TxnIntent;
import io.jimdb.pb.Txn.TxnStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import reactor.core.publisher.Flux;

/**
 * Define implement for transaction.
 *
 * @version V1.0
 */
public final class JimTransaction implements Transaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(JimTransaction.class);

  private static final ValueAccessor[][] ROW_VALUE_ACCESSORS_ARRAY_EMPTY = { QueryExecResult.EMPTY_ROW };

  private static final SelectFunc SELECT_FUNC = TxnHandler::select;
  private static final SelectFlowKeysFunc SELECT_KEYS_FLOW_FUNC = TxnHandler::selectFlowForKeys;
  private static final SelectFlowRangeFunc SELECT_RANGE_FLOW_FUNC = TxnHandler::selectFlowForRange;
  private static final SelectFlowFunc SELECT_FLOW_STREAM_FUNC = TxnHandler::selectFlowStream;
  private static final boolean GATHER_TRACE = false;

  private final Session session;
  private TxnConfig config;
  private RouterManager routerManager;
  private DistSender distSender;
  private IDAllocator idAllocator;

  private volatile TxnStatus status = TxnStatus.TXN_INIT;

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
      } else if (!(autoValue instanceof UnsignedLongValue)
              || ((UnsignedLongValue) autoValue).getValue().signum() < 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "auto_increment except unsigned long");
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

    for (Index index : indexes) {
      KvPair idxKvPair = Codec.encodeIndexKV(index, colValues, recordKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[insert]txn {} encode index [{}]", this.config.getTxnId(), idxKvPair);
      }
      this.config.addIntent(cvtKVtoIntent(idxKvPair, OpType.INSERT, true, 0), table);
    }
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
      Value[] oldData = accessValues(storeCtx, row, rows.getColumns());
      Value[] newData = computeNewValues(session, table, oldData, assignFlag, row, assignments);
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
        this.config.addIntent(cvtKVtoIntent(newIdxKvPair, OpType.INSERT, false, recordVersion.getValue()), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[update]txn {} encode new record:[{}], version：{}", this.config.getTxnId(),
                  newIdxKvPair, recordVersion.getValue());
        }
        continue;
      }

      KvPair oldIdxKvPair = Codec.encodeIndexKV(index, oldData, recordKey);

      Flux<Long> txnScanFlux = scanVerFunc.apply(storeCtx, oldIdxKvPair.getKey(), index)
              .onErrorResume(TxnHandler.getErrHandler(storeCtx, scanVerFunc, oldIdxKvPair.getKey(), index)).map(v -> {
                //value must be equal, so compare with key
                if (oldIdxKvPair.getKey().equals(newIdxKvPair.getKey())) {
                  this.config.addIntent(cvtKVtoIntent(oldIdxKvPair, OpType.INSERT, false, v), table);
                } else {
                  this.config.addIntent(cvtKVtoIntent(oldIdxKvPair, OpType.DELETE, false, v), table);
                  this.config.addIntent(cvtKVtoIntent(newIdxKvPair, OpType.INSERT, true, 0), table);
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
        this.config.addIntent(cvtKVtoIntent(oldKvPair, OpType.DELETE, false, recordVersion.getValue()), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[delete]txn {} encode old index:[{}], version:{} ", this.config.getTxnId(),
                  oldIdxKey, recordVersion.getValue());
        }
        continue;
      }

      //scan version
      Flux<Long> txnScanFlux = scanVerFunc.apply(storeCtx, oldIdxKey, index)
              .onErrorResume(TxnHandler.getErrHandler(storeCtx, scanVerFunc, oldIdxKey, index))
              .map(v -> onErr(table, oldIdxKey, oldKvPair, v));

      if (versionFlux == null) {
        versionFlux = txnScanFlux;
      } else {
        versionFlux = versionFlux.flatMap(v -> txnScanFlux);
      }
    }
    return versionFlux;
  }

  private Long onErr(Table table, ByteString oldIdxKey, KvPair oldKvPair, Long v) {
    this.config.addIntent(cvtKVtoIntent(oldKvPair, OpType.DELETE, false, v), table);
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
      SelectRequest.Builder reqBuilder = SelectRequest.newBuilder()
              .addAllFieldList(getSelectFields(resultColumns));
      if (index.isPrimary()) {
        reqBuilder.setKey(Codec.encodeKey(table.getId(), index.getColumns(), indexVals));
        subResult = SELECT_FUNC.apply(storeCtx, reqBuilder, resultColumns)
                .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FUNC, reqBuilder, resultColumns));
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
        ScanRequest.Builder request = ScanRequest.newBuilder()
                .setStartKey(idxKey)
                .setEndKey(Codec.nextKey(idxKey))
                .setOnlyOne(true);
        Flux<List<ByteString>> searchPkFlux = scanPkFunc.apply(storeCtx, request, index)
                .onErrorResume(TxnHandler.getErrHandler(storeCtx, scanPkFunc, request, index));
        subResult = searchPkFlux.flatMap(pks -> {
          if (pks == null || pks.isEmpty()) {
            return Flux.just(ROW_VALUE_ACCESSORS_ARRAY_EMPTY);
          }
          if (pks.size() > 1) {
            throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_UNKNOWN_IN_INDEX, index.getName());
          }
          reqBuilder.setKey(Codec.encodeTableKey(table.getId(), pks.get(0)));
          return SELECT_FUNC.apply(storeCtx, reqBuilder, resultColumns)
                  .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FUNC, reqBuilder, resultColumns));
        });
      } else {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "NOT supported with non-unique condition index.");
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
  public Flux<ExecResult> select(Table table, List<Processor.Builder> processors, ColumnExpr[] resultColumns,
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
  public Flux<ExecResult> select(Index index, List<Processor.Builder> processors, ColumnExpr[] resultColumns,
                                 List<Integer> outputOffsetList, List<ValueRange> ranges) throws JimException {
    if (processors == null || processors.isEmpty()) {
      return changeRowsToValueAccessor(resultColumns, null);
    }

    boolean isPrimary = index.isPrimary();
    boolean isOptimizeKey = isPrimary || index.isUnique();
    List<ByteString> keys = EMPTY_LIST;
    List<KvPair> rangePairs;
    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges, isOptimizeKey);
    if (isOptimizeKey && !kvPairs.isEmpty()) {   // when primary-key or unique could use both of key and range
      rangePairs = new ArrayList<>(kvPairs.size());
      keys = new ArrayList<>(kvPairs.size());
      for (KvPair kvPair : kvPairs) {
        if (kvPair.isKey()) {
          keys.add(kvPair.getKey());
        } else {
          rangePairs.add(kvPair);
        }
      }
    } else {
      rangePairs = kvPairs;
    }

    StoreCtx storeCtx = getStoreCtx(index.getTable());
    Flux<ValueAccessor[]> valuesFlux = getSelectFluxForKeys(processors, resultColumns, outputOffsetList, storeCtx, keys, isPrimary);
    for (KvPair kvPair : rangePairs) {
      Flux<ValueAccessor[]> valuesEachRangeFlux = getSelectFluxForRange(processors, resultColumns, outputOffsetList,
              storeCtx, kvPair, isPrimary);
      if (valuesFlux == null) {
        valuesFlux = valuesEachRangeFlux;
      } else {
        valuesFlux = valuesFlux.zipWith(valuesEachRangeFlux, (f1, f2) -> getValueAccessors(f1, f2));
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

  private Flux<ValueAccessor[]> getSelectFluxForRange(List<Processor.Builder> processors,
                                                      ColumnExpr[] resultColumns, List<Integer> outputOffsetList,
                                                      StoreCtx storeCtx, KvPair kvPair, boolean isPrimary) {
    SelectFlowRequest.Builder builder;
    if (isPrimary) {
      builder = getTableReaderForRange(processors, outputOffsetList, kvPair);
    } else {
      builder = getIndexReaderForRange(processors, outputOffsetList, kvPair);
    }

    return SELECT_RANGE_FLOW_FUNC.apply(storeCtx, builder, resultColumns, kvPair)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_RANGE_FLOW_FUNC, builder, resultColumns, kvPair));
  }

  private Flux<ValueAccessor[]> getSelectFluxForKeys(List<Processor.Builder> processors, ColumnExpr[] resultColumns,
                                                     List<Integer> outputOffsetList, StoreCtx storeCtx,
                                                     List<ByteString> keys, boolean isPrimary) {
    if (keys.isEmpty()) {
      return null;
    }
    SelectFlowRequest.Builder builder = isPrimary ? getTableReaderReqForKeys(processors, outputOffsetList, keys)
            : getIndexReaderReqForKeys(processors, outputOffsetList, keys);
    return SELECT_KEYS_FLOW_FUNC.apply(storeCtx, builder, resultColumns, keys)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_KEYS_FLOW_FUNC, builder, resultColumns, keys));
  }

  private SelectFlowRequest.Builder getIndexReaderForRange(List<Processor.Builder> processors,
                                                           List<Integer> outputOffsetList, KvPair kvPair) {
    SelectFlowRequest.Builder builder = SelectFlowRequest.newBuilder();
    for (Processor.Builder processor : processors) {
      if (processor.getType() == ProcessorType.INDEX_READ_TYPE) {
        IndexRead.Builder indexRead = IndexRead.newBuilder(processor.getIndexRead())
                .setType(KeyType.KEYS_RANGE_TYPE)
                .setRange(KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setIndexRead(indexRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private SelectFlowRequest.Builder getTableReaderForRange(List<Processor.Builder> processors,
                                                           List<Integer> outputOffsetList,
                                                           KvPair kvPair) {
    SelectFlowRequest.Builder builder = SelectFlowRequest.newBuilder();
    for (Processor.Builder processor : processors) {
      if (processor.getType() == ProcessorType.TABLE_READ_TYPE) {
        TableRead.Builder tableRead = TableRead.newBuilder(processor.getTableRead())
                .setType(KeyType.KEYS_RANGE_TYPE)
                .setRange(KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private SelectFlowRequest.Builder getTableReaderReqForKeys(List<Processor.Builder> processors,
                                                             List<Integer> outputOffsetList, List<ByteString> keys) {
    SelectFlowRequest.Builder builder = SelectFlowRequest.newBuilder();
    for (Processor.Builder processor : processors) {
      if (processor.getType() == ProcessorType.TABLE_READ_TYPE) {
        TableRead.Builder tableRead = TableRead.newBuilder(processor.getTableRead())
                .setType(KeyType.PRIMARY_KEY_TYPE)
                .addAllPkKeys(keys);
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private SelectFlowRequest.Builder getIndexReaderReqForKeys(List<Processor.Builder> processors,
                                                             List<Integer> outputOffsetList, List<ByteString> keys) {
    SelectFlowRequest.Builder builder = SelectFlowRequest.newBuilder();
    for (Processor.Builder processor : processors) {
      if (processor.getType() == ProcessorType.INDEX_READ_TYPE) {
        IndexRead.Builder indexRead = IndexRead.newBuilder(processor.getIndexRead())
                .setType(KeyType.PRIMARY_KEY_TYPE)
                .addAllIndexKeys(keys);
        processor.setIndexRead(indexRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
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
  public byte[] addIndex(Index index, byte[] startKey, byte[] endKey, int limit) {
    ColumnExpr[] columns = this.buildIndexColumns(index);
    ByteString lastKey = NettyByteString.wrap(endKey);
    SelectFlowRequest.Builder selectBuilder = this.buildIndexSelect(columns, NettyByteString.wrap(startKey), lastKey, limit);
    ValueAccessor[] values = this.selectByStream(index.getTable(), selectBuilder, columns).blockFirst();
    if (values != null && values.length > 0) {
      this.doAddIndex(index, columns, values);
      lastKey = selectBuilder.getProcessors(0).getTableRead().getRange().getEndKey();
    }

    lastKey = Codec.nextKey(lastKey);
    return lastKey == null ? endKey : NettyByteString.asByteArray(lastKey);
  }

  private ColumnExpr[] buildIndexColumns(Index index) {
    final Column[] pkColumns = index.getTable().getPrimary();
    final Column[] idxColumns = index.getColumns();
    final ColumnExpr[] exprs = new ColumnExpr[pkColumns.length + idxColumns.length];

    int i = 0;
    for (Column column : pkColumns) {
      exprs[i++] = new ColumnExpr(Long.valueOf(i), column);
    }
    for (Column column : idxColumns) {
      exprs[i++] = new ColumnExpr(Long.valueOf(i), column);
    }
    return exprs;
  }

  private SelectFlowRequest.Builder buildIndexSelect(ColumnExpr[] columns, ByteString startKey, ByteString endKey, int limit) {
    List<Exprpb.ColumnInfo> selCols = new ArrayList<>(columns.length);
    for (ColumnExpr column : columns) {
      Exprpb.ColumnInfo.Builder columnInfo = Exprpb.ColumnInfo.newBuilder()
              .setId(column.getId().intValue())
              .setTyp(column.getResultType().getType())
              .setUnsigned(column.getResultType().getUnsigned());
      if (column.getReorgValue() != null) {
        columnInfo.setReorgValue(column.getReorgValue());
      }
      selCols.add(columnInfo.build());
    }

    List<Integer> outputOffsetList = new ArrayList<>(columns.length);
    for (int i = 0; i < columns.length; i++) {
      outputOffsetList.add(i);
    }

    Processor.Builder tableReader = Processor.newBuilder()
            .setType(ProcessorType.TABLE_READ_TYPE)
            .setTableRead(TableRead.newBuilder()
                    .setType(KeyType.KEYS_RANGE_TYPE)
                    .addAllColumns(selCols)
                    .setRange(KeyRange.newBuilder().setStartKey(startKey).setEndKey(endKey))
            );
    Processor.Builder limitReader = Processor.newBuilder()
            .setType(ProcessorType.LIMIT_TYPE)
            .setLimit(Limit.newBuilder()
                    .setCount(limit));

    return SelectFlowRequest.newBuilder()
            .addProcessors(tableReader)
            .addProcessors(limitReader)
            .addAllOutputOffsets(outputOffsetList)
            .setGatherTrace(GATHER_TRACE);
  }

  private void doAddIndex(Index index, ColumnExpr[] columns, ValueAccessor[] values) {
    Table table = index.getTable();
    Map<Integer, Integer> colMap = getMapping(columns);
    StoreCtx storeCtx = getStoreCtx(table);
    Map<ByteString, Boolean> indexDulpMap = new HashMap<>(values.length);

    for (ValueAccessor value : values) {
      Value[] partOldValue = accessValues(value, table.getWritableColumns(), colMap);
      ByteString rowKey = Codec.encodeKey(table.getId(), table.getPrimary(), partOldValue);
      KvPair rowKvPair = new KvPair(rowKey);
      KvPair idxKvPair = Codec.encodeIndexKV(index, partOldValue, rowKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[addIndex]txn {} encode index [{}]", this.config.getTxnId(), idxKvPair);
      }

      Boolean flag = indexDulpMap.get(idxKvPair.getKey());
      if (flag != null && flag.booleanValue()) {
        LOGGER.error("add index {}.{} duplicate, {}", table.getName(), index.getName(),
                Arrays.toString(idxKvPair.getKey().toByteArray()));
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY, Arrays.toString(idxKvPair.getKey().toByteArray()), "INDEX");
      } else {
        indexDulpMap.put(idxKvPair.getKey(), Boolean.TRUE);
      }
      Boolean exist = this.txnScanForExist(storeCtx, idxKvPair, index).blockFirst();
      if (exist != null && exist.booleanValue()) {
        continue;
      }

      LongValue recordVersion = (LongValue) value.get(columns.length);
      //row add lock,
      this.config.addIntent(cvtKVtoIntent(rowKvPair, OpType.LOCK, false, recordVersion.getValue()), table);
      //index add lock
      this.config.addIntent(cvtKVtoIntent(idxKvPair, OpType.INSERT, true, 0), table);
    }
  }

  private Flux<ValueAccessor[]> selectByStream(Table table, SelectFlowRequest.Builder builder, ColumnExpr[] resultColumns) throws JimException {
    StoreCtx storeCtx = getStoreCtx(table);
    KeyRange keyRange = builder.getProcessors(0).getTableRead().getRange();
    KvPair kvPair = new KvPair(keyRange.getStartKey(), keyRange.getEndKey());
    return SELECT_FLOW_STREAM_FUNC.apply(storeCtx, builder, resultColumns, kvPair)
            .onErrorResume(TxnHandler.getErrHandler(storeCtx, SELECT_FLOW_STREAM_FUNC, builder, resultColumns, kvPair));
  }

  @Override
  public Flux<ExecResult> commit() throws JimException {
    if (!isPending()) {
      changeTxnStatus(TxnStatus.COMMITTED);
      close();
      return Flux.just(AckExecResult.getInstance());
    }

    changeTxnStatus(TxnStatus.COMMITTED);
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

    changeTxnStatus(TxnStatus.ABORTED);
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

  private Value[] accessValues(StoreCtx storeCtx, ValueAccessor accessor, ColumnExpr[] columns) {
    int colLength = columns.length;
    Value[] values = new Value[colLength];
    for (int i = 0; i < colLength; i++) {
      ColumnExpr expr = columns[i];
      Value value = expr.exec(accessor);
      if (expr.getResultType().getType() == Basepb.DataType.TimeStamp && null != value && value.getType() != ValueType.NULL) {
        value = DateValue.convertTimeZone((DateValue) value, storeCtx.getTimeZone(), TimeUtil.UTC_ZONE);
      }
      values[expr.getOffset()] = value;
    }
    return values;
  }

  private Value[] computeNewValues(Session session, Table table, Value[] oldValues, boolean[] assignFlag,
                                   ValueAccessor values, Assignment[] assignments) {
    Value[] newValues = cloneValue(oldValues);
    // An auto-updated column remains unchanged if all other columns are set to their current values.
    Map<Integer, Boolean> changes = new HashMap<>();
    Value autoUpdateValue = null;
    Value newValue = null;
    for (int i = 0; i < assignments.length; i++) {
      Assignment assignment = assignments[i];
      ColumnExpr columnExpr = assignment.getColumn();
      try {
        Value oldValue = oldValues[columnExpr.getOffset()];
        Value value = assignment.getExpression().exec(values);
        SQLType sqlType = columnExpr.getResultType();
        newValue = ValueConvertor.convertType(session, value, sqlType);
        //todo verify that auto-updated column are extracted
        if (newValue == null && sqlType.getOnUpdate()
                && (sqlType.getType() == Basepb.DataType.TimeStamp || sqlType.getType() == Basepb.DataType.DateTime)) {
          continue;
        }
        handleColumnNull(sqlType, columnExpr.getOriCol(), newValue);
        if (oldValue.compareTo(null, newValue) != 0) {
          changes.put(columnExpr.getOffset(), true);
        }
        ValueChecker.checkValue(sqlType, newValue);
        newValues[columnExpr.getOffset()] = newValue;
        values.set(columnExpr.getOffset(), newValue);
      } catch (JimException ex) {
        this.handleException(columnExpr.getOriCol(), newValue, i, ex);
      }
    }

    for (Column column : table.getWritableColumns()) {
      Basepb.DataType type = column.getType().getType();

      // not OpUpdate or not type of timestamp/datetime
      if (!column.getType().getOnUpdate() || (type != Basepb.DataType.TimeStamp && type != Basepb.DataType.DateTime)) {
        continue;
      }

      // user updated type of timestamp/datetime
      boolean changed = changes.containsKey(column.getOffset());
      if (changed) {
        continue;
      }
      if (changes.isEmpty()) {
        continue;
      }
      autoUpdateValue = DateValue.getNow(type, column.getType().getScale(), session.getStmtContext().getLocalTimeZone());

      assignFlag[column.getOffset()] = true;
      newValues[column.getOffset()] = autoUpdateValue;
      values.set(column.getOffset(), autoUpdateValue);
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

  private List<SelectField> getSelectFields(ColumnExpr[] resultColumns) {
    List<SelectField> selectFields = new ArrayList<>(resultColumns.length);
    //select fields
    for (ColumnExpr column : resultColumns) {
      SelectField.Builder field = SelectField.newBuilder()
              .setColumn(convertColumnBuilder(column))
              .setTyp(SelectField.Type.Column);
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
  }

  private void changeTxnStatus(TxnStatus newStatus) throws JimException {
    if (newStatus != TxnStatus.TXN_INIT
            && newStatus != TxnStatus.COMMITTED
            && newStatus != TxnStatus.ABORTED) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_STATE_INVALID, newStatus.name());
    }
    TxnStatus oldStatus = this.status;
    this.status = newStatus;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("change transaction status from {} to {}", oldStatus, newStatus);
    }
  }

  public Flux<Long> txnScanForVersion(StoreCtx storeCtx, ByteString startKey, Index index) {
    ByteString endKey = Codec.nextKey(startKey);
    ScanRequest.Builder request = ScanRequest.newBuilder()
            .setStartKey(startKey)
            .setEndKey(endKey)
            .setOnlyOne(true);

    return TxnHandler.txnScan(storeCtx, request)
            .map(kvs -> {
              if (kvs.size() != 1) {
                throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
              }
              if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                LOGGER.error("txnScanForVersion: ctx {}  scan result {}", storeCtx.getCxtId(), kvs);
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

  public Flux<Boolean> txnScanForExist(StoreCtx storeCtx, KvPair idxKvPair, Index index) {
    ByteString endKey = Codec.nextKey(idxKvPair.getKey());
    ScanRequest.Builder request = ScanRequest.newBuilder()
            .setStartKey(idxKvPair.getKey())
            .setEndKey(endKey)
            .setOnlyOne(true);

    return TxnHandler.txnScan(storeCtx, request)
            .map(kvs -> {
              if (kvs.isEmpty() || kvs.get(0) == null || kvs.get(0).getValue() == null) {
                return Boolean.FALSE;
              }
              if (kvs.size() > 1) {
                throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_VERSION_DUP, kvs.toString());
              }
              if (index.isUnique()) {
                ByteString value = kvs.get(0).getValue();
                if (!value.isEmpty() && ByteUtil.compare(idxKvPair.getValue(), value) != 0) {
                  LOGGER.error("add index {}.{} scan duplicate, {}", index.getTable().getName(), index.getName(),
                          Arrays.toString(idxKvPair.getKey().toByteArray()));
                  throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_ENTRY,
                          Arrays.toString(idxKvPair.getKey().toByteArray()), "INDEX");
                }
              }

              return Boolean.TRUE;
            });
  }

  public Flux<List<ByteString>> txnScanForPk(StoreCtx storeCtx, ScanRequest.Builder request, Index index) {
    request.setMaxCount(10000);
    return TxnHandler.txnScan(storeCtx, request).map(kvs -> {
      if (kvs.isEmpty()) {
        return EMPTY_LIST;
      }

      List<ByteString> pkValuesList = new ArrayList<>(kvs.size());
      for (KeyValue kv : kvs) {
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

  private TxnIntent cvtKVtoIntent(KvPair kvPair, OpType opType, boolean check, long version) {
    TxnIntent.Builder txnIntent = TxnIntent.newBuilder()
            .setTyp(opType)
            .setKey(kvPair.getKey());
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
    this.changeTxnStatus(TxnStatus.TXN_INIT);
    return oldConfig;
  }

  private void sendTask(TxnAsyncTask runnable) {
    if (runnable != null) {
      this.distSender.asyncTask(runnable);
    }
  }
}
