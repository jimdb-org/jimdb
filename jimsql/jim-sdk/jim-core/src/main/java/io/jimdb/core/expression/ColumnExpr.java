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
package io.jimdb.core.expression;

import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.JsonValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.core.values.YearValue;
import io.jimdb.pb.Basepb.DataType;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS" })
public final class ColumnExpr extends Expression {
  private int offset = -1;
  private Integer id;
  private Long uid;
  private String catalog;
  private String oriTable;
  private String aliasTable;
  private String oriCol;
  private String aliasCol;
  private boolean inSubQuery;
  private ByteString reorgValue;
  private Value defaultValue;

  public ColumnExpr(Long uid) {
    this.id = Integer.valueOf(0);
    this.uid = uid;
  }

  public ColumnExpr(Long uid, Column column) {
    if (column == null) {
      return;
    }

    this.uid = uid;
    this.id = column.getId();
    this.offset = column.getOffset();
    this.oriCol = column.getName();
    this.aliasCol = this.oriCol;
    final Table tbl = column.getTable();
    if (tbl != null) {
      this.oriTable = tbl.getName();
      this.aliasTable = this.oriTable;
      this.catalog = tbl.getCatalog() == null ? "" : tbl.getCatalog().getName();
    }
    this.resultType = column.getType();
    this.reorgValue = column.getReorgValue();
    this.defaultValue = column.getDefaultValue();
  }

  @Override
  public ColumnExpr clone() {
    ColumnExpr result = new ColumnExpr(this.uid);
    result.offset = this.offset;
    result.id = this.id;
    result.catalog = this.catalog;
    result.oriTable = this.oriTable;
    result.aliasTable = this.aliasTable;
    result.oriCol = this.oriCol;
    result.aliasCol = this.aliasCol;
    result.inSubQuery = this.inSubQuery;
    result.reorgValue = this.reorgValue;
    clone(result);
    return result;
  }

  public void setOther(ColumnExpr otherExpr) {
    this.uid = otherExpr.uid;
    this.offset = otherExpr.offset;
    this.id = otherExpr.id;
    this.catalog = otherExpr.catalog;
    this.oriTable = otherExpr.oriTable;
    this.aliasTable = otherExpr.aliasTable;
    this.oriCol = otherExpr.oriCol;
    this.aliasCol = otherExpr.aliasCol;
    this.inSubQuery = otherExpr.isInSubQuery();
    this.reorgValue = otherExpr.getReorgValue();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ColumnExpr)) {
      return false;
    }
    ColumnExpr otherCol = (ColumnExpr) obj;

    return this.uid != null && this.uid.equals(otherCol.uid);
  }

  @Override
  public int hashCode() {
    int result = offset;
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (uid != null ? uid.hashCode() : 0);
    result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
    result = 31 * result + (oriTable != null ? oriTable.hashCode() : 0);
    result = 31 * result + (aliasTable != null ? aliasTable.hashCode() : 0);
    result = 31 * result + (oriCol != null ? oriCol.hashCode() : 0);
    result = 31 * result + (aliasCol != null ? aliasCol.hashCode() : 0);
    return 31 * result + (inSubQuery ? 1 : 0);
  }

  @Override
  public ExpressionType getExprType() {
    return ExpressionType.COLUMN;
  }

  @Override
  public List<Point> convertToPoints(Session session) {
    //TODO value zero
    Point start1 = new Point(Value.MAX_VALUE, true);
    Point end1 = new Point(LongValue.getInstance(0), false, false);

    Point start2 = new Point(LongValue.getInstance(0), true, false);
    Point end2 = new Point(Value.MIN_VALUE, false);

    return Lists.newArrayList(start1, end1, start2, end2);
  }

  @Override
  public boolean check(ConditionChecker conditionChecker) {
    return conditionChecker.getColumnId() == id;
  }

  @Override
  public Expression resolveOffset(Schema schema, boolean isClone) throws JimException {
    ColumnExpr result = isClone ? this.clone() : this;

    result.offset = schema.getColumnIndex(result);
    if (result.offset == -1) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, oriCol, oriTable);
    }
    return result;
  }

  @Override
  public Value exec(ValueAccessor accessor) throws JimException {
    Value v = accessor.get(offset);
    if (!v.isNull()) {
      v = ValueConvertor.convertType(null, v, resultType);
    }
    return v;
  }

  @Override
  public LongValue execLong(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToLong(session, v, resultType);
  }

  @Override
  public UnsignedLongValue execUnsignedLong(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToUnsignedLong(session, v, null);
  }

  @Override
  public DoubleValue execDouble(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToDouble(session, v, null);
  }

  @Override
  public DecimalValue execDecimal(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToDecimal(session, v, null);
  }

  @Override
  public StringValue execString(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    if (v.isNull()) {
      return null;
    }

    if (resultType.getType() == DataType.Char || resultType.getType() == DataType.NChar) {
      String str = v.getString();
      if (str.length() < resultType.getPrecision()) {
        str = StringUtils.rightPad(str, (int) resultType.getPrecision() - str.length());
        return StringValue.getInstance(str);
      }
    }
    return ValueConvertor.convertToString(session, v, null);
  }

  @Override
  public DateValue execDate(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToDate(session, v, null);
  }

  @Override
  public TimeValue execTime(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToTime(session, v, null);
  }

  @Override
  public YearValue execYear(Session session, ValueAccessor accessor) throws JimException {
    final Value v = accessor.get(offset);
    return v.isNull() ? null : ValueConvertor.convertToYear(session, v, null);
  }

  @Override
  public JsonValue execJson(Session session, ValueAccessor accessor) throws JimException {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "Json");
  }

  public int getOffset() {
    return offset;
  }

  public ColumnExpr setOffset(int offset) {
    this.offset = offset;
    return this;
  }

  public Integer getId() {
    return id;
  }

  public ColumnExpr setId(Integer id) {
    this.id = id;
    return this;
  }

  public Long getUid() {
    return uid;
  }

  public ColumnExpr setUid(Long uid) {
    this.uid = uid;
    return this;
  }

  public String getCatalog() {
    return catalog;
  }

  public ColumnExpr setCatalog(String catalog) {
    this.catalog = catalog;
    return this;
  }

  public String getOriTable() {
    return oriTable;
  }

  public ColumnExpr setOriTable(String oriTable) {
    this.oriTable = oriTable;
    return this;
  }

  public String getAliasTable() {
    return aliasTable;
  }

  public ColumnExpr setAliasTable(String aliasTable) {
    this.aliasTable = aliasTable;
    return this;
  }

  public String getOriCol() {
    return oriCol;
  }

  public ColumnExpr setOriCol(String oriCol) {
    this.oriCol = oriCol;
    return this;
  }

  public String getAliasCol() {
    return aliasCol;
  }

  public ColumnExpr setAliasCol(String aliasCol) {
    this.aliasCol = aliasCol;
    return this;
  }

  public boolean isInSubQuery() {
    return inSubQuery;
  }

  public ColumnExpr setInSubQuery(boolean inSubQuery) {
    this.inSubQuery = inSubQuery;
    return this;
  }

  public ByteString getReorgValue() {
    return reorgValue;
  }

  public Value getDefaultValue() {
    return defaultValue;
  }

  public void setReorgValue(ByteString reorgValue) {
    this.reorgValue = reorgValue;
  }

  @Override
  public String toString() {
    String result = StringUtils.isEmpty(this.aliasCol) ? this.oriCol : this.aliasCol;
    if (StringUtils.isNotEmpty(this.aliasTable)) {
      result = this.aliasTable + "." + result;
    }
    if (StringUtils.isNotEmpty(this.catalog)) {
      result = this.catalog + "." + result;
    }

    return result;
  }
}
