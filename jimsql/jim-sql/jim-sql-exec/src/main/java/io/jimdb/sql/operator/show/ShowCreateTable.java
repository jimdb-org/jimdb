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
package io.jimdb.sql.operator.show;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Basepb.StoreType;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.Value;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * ShowCreateTable
 *
 * @since 2020/1/8
 */
@SuppressFBWarnings({ "LEST_LOST_EXCEPTION_STACK_TRACE", "CC_CYCLOMATIC_COMPLEXITY" })
public class ShowCreateTable extends RelOperator {
  private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
  private static final String ZERO_DATETIME = "0000-00-00 00:00:00";

  private final String dbName;
  private final String tableName;

  public ShowCreateTable(String dbName, String tableName) {
    this.dbName = dbName;
    this.tableName = tableName;
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

    Column pkColumn = null;
    boolean existsAutoIncId = false;
    Column[] columns = table.getReadableColumns();
    List<ValueAccessor> rows = new ArrayList<>(1);
    String line = System.getProperty("line.separator");
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("CREATE TABLE `%s`(", table.getName())).append(line);

    for (int i = 0; i < columns.length; i++) {
      Column column = columns[i];
      builder.append(String.format(" `%s` %s", column.getName(), column.getTypeDesc()));

      if (column.isAutoIncr()) {
        existsAutoIncId = true;
        builder.append(" NOT NULL AUTO_INCREMENT");
      } else {
        if (!column.isNullable()) {
          builder.append(" NOT NULL");
        }
        if (column.hasDefaultValue()) {
          Value defaultValue = column.getDefaultValue();
          if (defaultValue instanceof NullValue) {
            if (column.isNullable()) {
              if (column.getType().getType() == DataType.TimeStamp) {
                builder.append(" NULL");
              }
              builder.append(" DEFAULT NULL");
            }
          } else if (defaultValue instanceof StringValue
                  && CURRENT_TIMESTAMP.equalsIgnoreCase(((StringValue) defaultValue).getValue())) {
            builder.append(" DEFAULT CURRENT_TIMESTAMP");
            if (column.getType().getScale() > 0) {
              builder.append(String.format("(%d)", column.getType().getScale()));
            }
          } else {
            String defaultValueStr = defaultValue.getString();
            if (column.getType().getType() == DataType.TimeStamp && !ZERO_DATETIME.equals(defaultValueStr)) {
              if (column.getDefaultValue() instanceof TimeValue) {
                TimeValue value = (TimeValue) column.getDefaultValue();
                defaultValueStr = value.convertToString();
              }
            }

            //              if (column.getType().getType() == DataType.Bit) {
//
//              } else {
            builder.append(String.format(" DEFAULT '%s'", defaultValueStr));
//              }
          }
        }
        if (column.isOnUpdate()) {
          builder.append(" ON UPDATE CURRENT_TIMESTAMP").append(column.scaleToStr());
        }
      }

      if (StringUtils.isNotBlank(column.getComment())) {
        builder.append(String.format(" COMMENT '%s'", column.getComment()));
      }
      if (i != columns.length - 1) {
        builder.append(String.format(",%s", line));
      }
      if (column.isPrimary() || column.hasPrimaryKeyFlag()) {
        pkColumn = column;
      }
    }

    Index[] indices = table.getReadableIndices();
    List<Index> publicIndices = Arrays.stream(indices).filter(index -> index.getState() == MetaState.Public).collect(Collectors.toList());
    if (!publicIndices.isEmpty()) {
      builder.append(String.format(",%s", line));
    }
    for (int i = 0; i < publicIndices.size(); i++) {
      Index publicIndex = publicIndices.get(i);
      if (publicIndex.isPrimary()) {
        builder.append("  PRIMARY KEY ");
      } else if (publicIndex.isUnique()) {
        builder.append(String.format("  UNIQUE KEY `%s`", publicIndex.getName()));
      } else {
        builder.append(String.format("  KEY `%s`", publicIndex.getName()));
      }

      Column[] publicIndexColumns = publicIndex.getColumns();
      List<String> colStrs = new ArrayList<>(publicIndexColumns.length);
      for (Column indexColumn : publicIndexColumns) {
//        if (indexColumn.getType().getScale() != Types.UNDEFINE_WIDTH) {
//          colStrs.add(String.format("`%s`(%d)", indexColumn.getName(), indexColumn.getType().getScale()));
//        } else {
        colStrs.add(String.format("`%s`", indexColumn.getName()));
//        }
      }

      if (!colStrs.isEmpty()) {
        builder.append(String.format("(%s)", StringUtils.join(colStrs, ",")));
      }
      if (i != publicIndices.size() - 1) {
        builder.append(String.format(",%s", line));
      }
    }

    String engine = "UNKNOWN";
    if (table.getEngine() == StoreType.Store_Hot) {
      engine = "MEMORY";
    } else if (table.getEngine() == StoreType.Store_Warm) {
      engine = "DISK";
    } else if (table.getEngine() == StoreType.Store_Mix) {
      engine = "MIX";
    }
    builder.append(String.format("%s", line)).append(String.format(") ENGINE=%s", engine));

    if (existsAutoIncId) {
      long nextAutoIncId = 0;
      if (nextAutoIncId > 1) {
        builder.append(String.format(" AUTO_INCREMENT=%d", nextAutoIncId));
      }
    }
    if (StringUtils.isNotBlank(table.getComment())) {
      builder.append(String.format(" COMMENT=%s", table.getComment()));
    }
    // PARTITION BY

    Value[] values = new Value[]{
            StringValue.getInstance(table.getName()),
            StringValue.getInstance(builder.toString())
    };
    RowValueAccessor row = new RowValueAccessor(values);
    rows.add(row);

    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]), rows.toArray(new ValueAccessor[0])));
  }
}
