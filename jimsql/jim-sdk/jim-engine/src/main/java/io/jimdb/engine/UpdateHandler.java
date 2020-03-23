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

package io.jimdb.engine;

import static io.jimdb.engine.ScanHandler.SCAN_VER_FUNC;

import java.util.HashMap;
import java.util.Map;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.TimeUtil;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.meta.Router;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class UpdateHandler extends RequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateHandler.class);

  /**
   * TODO
   *
   * @param session     session
   * @param table       table
   * @param assignments assignments
   * @param rows        rows
   * @return
   * @throws BaseException
   */
  public static Flux<ExecResult> update(ShardSender shardSender, Router router, Session session, Table table, Assignment[] assignments, QueryResult rows) throws BaseException {
    if (rows == null || rows.size() == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int resultRowLength = rows.size();
    Index[] indexes = table.getWritableIndices();
    StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
    boolean[] assignFlag = new boolean[table.getWritableColumns().length];
    for (Assignment assignment : assignments) {
      assignFlag[assignment.getColumn().getOffset()] = true;
    }

    Flux<Long>[] versionFlux = new Flux[1];
    rows.forEach(row -> {
      LongValue recordVersion = (LongValue) row.get(row.size() - 1);
      Value[] oldData = extractValues(storeCtx, row, rows.getColumns());
      Value[] newData = computeNewValues(session, table, oldData, assignFlag, row, assignments);
      versionFlux[0] = updateRow(shardSender, session.getTxn(), table, indexes, storeCtx, versionFlux[0], assignFlag, recordVersion, oldData, newData);
    });

    ExecResult result = new DMLExecResult(resultRowLength);
    if (versionFlux[0] != null) {
      return versionFlux[0].map(v -> result);
    }
    return Flux.create(sink -> sink.next(result));
  }

  // TODO move this function under ValueAccessor
  private static Value[] extractValues(StoreCtx storeCtx, ValueAccessor accessor, ColumnExpr[] columns) {
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

  private static Value[] computeNewValues(Session session, Table table, Value[] oldValues, boolean[] assignFlag,
                                          ValueAccessor values, Assignment[] assignments) {
    Value[] newValues = new Value[oldValues.length];
    System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);

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
        Metapb.SQLType sqlType = columnExpr.getResultType();
        newValue = ValueConvertor.convertType(session, value, sqlType);
        //todo verify that auto-updated column are extracted
        if (newValue == null && sqlType.getOnUpdate()
                && (sqlType.getType() == Basepb.DataType.TimeStamp || sqlType.getType() == Basepb.DataType.DateTime)) {
          continue;
        }

        // check null column
        if (sqlType.getNotNull() && (newValue == null || newValue.isNull())) {
          throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_NULL_ERROR, columnExpr.getOriCol());
        }

        if (oldValue.compareTo(null, newValue) != 0) {
          changes.put(columnExpr.getOffset(), true);
        }
        ValueChecker.checkValue(sqlType, newValue);
        newValues[columnExpr.getOffset()] = newValue;
        values.set(columnExpr.getOffset(), newValue);
      } catch (BaseException ex) {
        ErrorHandler.handleException(columnExpr.getOriCol(), newValue, i, ex);
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

  private static Flux<Long> updateRow(ShardSender shardSender, Transaction transaction, Table table, Index[] indexes, StoreCtx storeCtx, Flux<Long> versionFlux,
                                      boolean[] assignFlag, LongValue recordVersion, Value[] oldData, Value[] newData) {

    ByteString recordKey = Codec.encodeKey(table.getId(), table.getPrimary(), newData);
    for (Index index : indexes) {
      if (!index.isPrimary() && !checkChange(index, assignFlag)) {
        continue;
      }
      KvPair newIdxKvPair = Codec.encodeIndexKV(index, newData, recordKey);
      if (index.isPrimary()) {
        transaction.addIntent(newIdxKvPair, Txn.OpType.INSERT, false, recordVersion.getValue(), table);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[update]txn {} encode new record:[{}], version：{}", transaction.getTxnId(),
              newIdxKvPair, recordVersion.getValue());
        }
        continue;
      }

      KvPair oldIdxKvPair = Codec.encodeIndexKV(index, oldData, recordKey);

      Flux<Long> txnScanFlux =
          SCAN_VER_FUNC.apply(shardSender, storeCtx, oldIdxKvPair.getKey(), index)
              .onErrorResume(ScanHandler.getErrHandler(shardSender, storeCtx, SCAN_VER_FUNC, oldIdxKvPair.getKey(), index)).map(v -> {
                //value must be equal, so compare with key
                if (oldIdxKvPair.getKey().equals(newIdxKvPair.getKey())) {
                  transaction.addIntent(oldIdxKvPair, Txn.OpType.INSERT, false, v, table);
                } else {
                  transaction.addIntent(oldIdxKvPair, Txn.OpType.DELETE, false, v, table);
                  transaction.addIntent(newIdxKvPair, Txn.OpType.INSERT, true, 0, table);
                }
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[update]txn {} encode old index:[{}], version:{} ; new index:[{}]", transaction.getTxnId(), oldIdxKvPair, v, newIdxKvPair);
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

  private static boolean checkChange(Index index, boolean[] assignFlag) {
    boolean changeFLag = false;
    for (Column column : index.getColumns()) {
      if (assignFlag[column.getOffset()]) {
        changeFLag = true;
        break;
      }
    }
    return changeFLag;
  }
}
