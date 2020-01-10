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
package io.jimdb.sql.operator.show;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.sql.ddl.DDLUtils;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * ShowColumns or DESC
 *
 * @since 2019/12/31
 */
@SuppressFBWarnings({ "LEST_LOST_EXCEPTION_STACK_TRACE" })
public final class ShowColumns extends RelOperator {
  private static final String CURRENT_TIMESTAMP = "current_timestamp";
  private static final String ZERO_DATETIME = "0000-00-00 00:00:00";
  private static final String DEFAULT_PRIVILEGES = "select,insert,update,references";

  private final String dbName;
  private final String tableName;
  private final boolean isFull;

  public ShowColumns(String dbName, String tableName, boolean isFull) {
    this.dbName = DDLUtils.trimName(dbName);
    this.tableName = DDLUtils.trimName(tableName);
    this.isFull = isFull;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    if (StringUtil.isBlank(dbName)) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NO_DB_ERROR, dbName);
    }
    if (StringUtil.isBlank(tableName)) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_WRONG_TABLE_NAME, tableName);
    }

    Table table;
    try {
      table = session.getTxnContext().getMetaData().getTable(dbName, tableName);
    } catch (DBException e) {
      if (e.getCode() == ErrorCode.ER_BAD_DB_ERROR || e.getCode() == ErrorCode.ER_BAD_TABLE_ERROR) {
        throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_NO_SUCH_TABLE, dbName, tableName);
      }
      throw e;
    }
    Column[] columns = table.getReadableColumns();
    List<ValueAccessor> rows = new ArrayList<>(columns.length);

    for (Column column : columns) {
      String nullable = column.isNullable() ? "YES" : "NO";
      String keyFlag = "";
      if (column.isPrimary() || column.hasPrimaryKeyFlag()) {
        keyFlag = "PRI";
      } else if (column.hasUniqueKeyFlag()) {
        keyFlag = "UNI";
      } else if (column.hasMultipleKeyFlag()) {
        keyFlag = "MUL";
      }
      String defaultValueStr = null;
      if (column.hasDefaultValue()) {
        Value defaultValue = column.getDefaultValue();
        if (defaultValue instanceof StringValue) {
          defaultValueStr = ((StringValue) defaultValue).getValue();
          if ((column.getType().getType() == DataType.TimeStamp
                  || column.getType().getType() == DataType.DateTime)
                  && CURRENT_TIMESTAMP.equalsIgnoreCase(defaultValueStr) && column.getType().getScale() > 0) {
            defaultValueStr = String.format("%s(%d)", defaultValueStr, column.getType().getScale());
          }
        } else {
          defaultValueStr = defaultValue.getString();
          if (column.getType().getType() == DataType.TimeStamp && !ZERO_DATETIME.equals(defaultValueStr)
                  && !defaultValueStr.toLowerCase().startsWith(CURRENT_TIMESTAMP)) {
            if (column.getDefaultValue() instanceof TimeValue) {
              TimeValue value = (TimeValue) column.getDefaultValue();
              defaultValueStr = value.convertToString();
            }
          }
//        if (column.getType().getType() == DataType.Bit) {
//
//        }
        }
      }

      String extra = "";
      if (column.isAutoIncr()) {
        extra = "auto_increment";
      } else if (column.isOnUpdate()) {
        extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP" + column.scaleToStr();
      }

      Value[] values;
      if (isFull) {
        boolean hasCharset = column.hasCharset();
        values = new Value[]{
                StringValue.getInstance(column.getName()),
                StringValue.getInstance(column.getTypeDesc()),
                hasCharset ? StringValue.getInstance(column.getCollation()) : NullValue.getInstance(),
                StringValue.getInstance(nullable),
                StringValue.getInstance(keyFlag),
                StringValue.getInstance(defaultValueStr),
                StringValue.getInstance(extra),
                StringValue.getInstance(DEFAULT_PRIVILEGES),
                StringValue.getInstance(column.getComment())
        };
      } else {
        values = new Value[]{
                StringValue.getInstance(column.getName()),
                StringValue.getInstance(column.getTypeDesc()),
                StringValue.getInstance(nullable),
                StringValue.getInstance(keyFlag),
                StringValue.getInstance(defaultValueStr),
                StringValue.getInstance(extra)
        };
      }
      RowValueAccessor row = new RowValueAccessor(values);
      rows.add(row);
    }

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]), rows.toArray(new ValueAccessor[0])));
  }
}
