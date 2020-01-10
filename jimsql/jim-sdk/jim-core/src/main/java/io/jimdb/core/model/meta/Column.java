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
package io.jimdb.core.model.meta;

import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public final class Column {
  private final Integer id;

  private final int offset;

  private final ColumnInfo columnInfo;

  private final Table table;

  private Value defaultValue;

  private ByteString reorgValue;

  public Column(Table table, ColumnInfo columnInfo, int offset) {
    this.table = table;
    this.columnInfo = columnInfo;
    this.id = columnInfo.getId();
    this.offset = offset;

    if (columnInfo.getHasDefault()) {
      String defValue = columnInfo.getDefaultValue().toStringUtf8();
      try {
        this.defaultValue = ValueConvertor.convertType(null, StringValue.getInstance(defValue), columnInfo.getSqlType());
      } catch (Exception ex) {
        this.defaultValue = NullValue.getInstance();
      }
    }
    this.reorgValue = columnInfo.getReorgValue();
  }

  public Integer getId() {
    return id;
  }

  public int getOffset() {
    return offset;
  }

  public String getName() {
    return columnInfo.getName();
  }

  public boolean isPrimary() {
    return columnInfo.getPrimary();
  }

  public boolean isAutoIncr() {
    return columnInfo.getAutoIncr();
  }

  public SQLType getType() {
    return columnInfo.getSqlType();
  }

  public Value getDefaultValue() {
    return defaultValue;
  }

  public ByteString getReorgValue() {
    return reorgValue;
  }

  public String getComment() {
    return columnInfo.getComment();
  }

  public boolean isNullable() {
    return !columnInfo.getSqlType().getNotNull();
  }

  public boolean hasDefaultValue() {
    return columnInfo.getHasDefault();
  }

  public boolean isOnUpdate() {
    return columnInfo.getSqlType().getOnUpdate();
  }

  public boolean hasPrimaryKeyFlag() {
    return (columnInfo.getKeyFlag() & Types.FLAG_KEY_PRIMARY) > 0;
  }

  public boolean hasUniqueKeyFlag() {
    return (columnInfo.getKeyFlag() & Types.FLAG_KEY_UNIQUE) > 0;
  }

  public boolean hasMultipleKeyFlag() {
    return (columnInfo.getKeyFlag() & Types.FLAG_KEY_MULTIPLE) > 0;
  }

  public String getCharset() {
    return columnInfo.getSqlType().getCharset();
  }

  public String getCollation() {
    return columnInfo.getSqlType().getCollate();
  }

  public boolean hasCharset() {
    Basepb.DataType dataType = columnInfo.getSqlType().getType();
    return dataType != Basepb.DataType.Binary && dataType != Basepb.DataType.VarBinary;
  }

  public String getTypeDesc() {
    SQLType sqlType = columnInfo.getSqlType();
    String desc = Types.toDescribe(columnInfo);
    if (sqlType.getUnsigned() && sqlType.getType() != Basepb.DataType.Bit
            && sqlType.getType() != Basepb.DataType.Year) {
      desc += " unsigned";
    }
    if (sqlType.getZerofill() && sqlType.getType() != Basepb.DataType.Year) {
      desc += " zerofill";
    }

    return desc;
  }

  public String scaleToStr() {
    int scale = columnInfo.getSqlType().getScale();
    return scale == 0 ? "" : String.format("(%d)", scale);
  }

  public Table getTable() {
    return table;
  }
}
