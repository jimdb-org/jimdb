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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.Assignment;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.core.values.ValueConvertor;
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
public class InsertHandler extends RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertHandler.class);

  public static Flux<ExecResult> insert(IdAllocator idAllocator, Session session, Table table, Column[] insertCols, Expression[][] rows, Assignment[] duplicate,
                                        boolean hasRefColumn) throws BaseException {
    if (rows == null || rows.length == 0) {
      return Flux.just(DMLExecResult.EMPTY);
    }

    int rowSize = rows.length;
    Column[] tblColumns = table.getWritableColumns();
    RowValueAccessor rowAccessor = new RowValueAccessor(null);
    Value[][] rowValues = new Value[rowSize][];
    for (int i = 0; i < rowSize; i++) {
      rowValues[i] = evalRow(session, tblColumns, insertCols, rows[i], rowAccessor, hasRefColumn, i);
    }

    Index[] indexes = table.getWritableIndices();
    Column[] pkColumns = table.getPrimary();

    long lastInsertId = backFillColumnIds(idAllocator, table.getCatalog().getId(), table.getId(), pkColumns[0], rowValues);

    for (Value[] row : rowValues) {
      insertRow(session.getTxn(), table, indexes, pkColumns, row);
    }

    return Flux.just(new DMLExecResult(rowSize, lastInsertId));
  }

  private static long backFillColumnIds(IdAllocator idAllocator, int dbId, int tableId, Column pkColumn, Value[][] rowValues) {
    if (!pkColumn.isAutoIncr()) {
      return DMLExecResult.EMPTY_INSERT_ID;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("start to get auto increment doc ids");
    }
    int offset = pkColumn.getOffset();
    List<Integer> indexList = new ArrayList<>(rowValues.length);
    for (int i = 0; i < rowValues.length; i++) {
      Value autoValue = rowValues[i][offset];
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
      rowValues[i][offset] = autoIncrIds.get(i);
    }
    return lastInsertId;
  }

  private static void insertRow(Transaction transaction, Table table, Index[] indexes, Column[] pkColumns, Value[] colValues) {
    //storage key
    ByteString recordKey = Codec.encodeKey(table.getId(), pkColumns, colValues);

    for (Index index : indexes) {
      KvPair idxKvPair = Codec.encodeIndexKV(index, colValues, recordKey);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[insert]txn {} encode index [{}]", transaction.getTxnId(), idxKvPair);
      }
      transaction.addIntent(idxKvPair, Txn.OpType.INSERT, true, 0, table);
    }
  }

  private static Value[] evalRow(Session session, Column[] tblCols, Column[] insertCols, Expression[] insertColExprs, RowValueAccessor rowAccessor,
                                 boolean hasRefColumn, int rowIdx) {
    int colSize = tblCols.length;
    Value[] result = new Value[colSize];
    boolean[] hasVal = new boolean[colSize];
    if (hasRefColumn) {
      setRefColumnValue(tblCols, result, hasVal);
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
    } catch (BaseException ex) {
      ErrorHandler.handleException(insertCols[i].getName(), colValue, rowIdx, ex);
    }

    fillRow(tblCols, result, hasVal, session);
    return result;
  }

  private static Value convertValue(Session session, Column column, Value value) {
    Metapb.SQLType sqlType = column.getType();
    Value result = ValueConvertor.convertType(session, value, sqlType);
    if (result == null || result.isNull()) {
      return NullValue.getInstance();
    }

    Basepb.DataType dataType = sqlType.getType();

    if (dataType == Basepb.DataType.Varchar || dataType == Basepb.DataType.Char
            || dataType == Basepb.DataType.NChar || dataType == Basepb.DataType.Text) {
      String str = result.getString();
      str = StringUtil.trimTail(str);
      result = StringValue.getInstance(str);
    }

    return result;
  }

  private static void setRefColumnValue(Column[] tblCols, Value[] row, boolean[] hasVal) {
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

  private static void fillRow(Column[] tblCols, Value[] row, boolean[] hasVal, Session session) {
    int offset;
    for (Column column : tblCols) {
      if (column.isAutoIncr()) {
        continue;
      }
      offset = column.getOffset();
      row[offset] = fillColumn(column, row[offset], hasVal[offset], session);
    }
  }

  private static Value fillColumn(Column column, Value value, boolean hasValue, Session session) {
    if (hasValue) {
      return value;
    }
    value = column.getDefaultValue();
    Metapb.SQLType sqlType = column.getType();

    if (sqlType.getOnInit()) {
      value = DateValue.getNow(sqlType.getType(), sqlType.getScale(), session.getStmtContext().getLocalTimeZone());
    }

    boolean notNull = sqlType.getNotNull();
    // check null column
    if (notNull && (value == null || value.isNull())) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_NULL_ERROR, column.getName());
    }

    return value;
  }
}
