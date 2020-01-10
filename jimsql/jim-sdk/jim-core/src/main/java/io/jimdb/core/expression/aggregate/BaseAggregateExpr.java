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
package io.jimdb.core.expression.aggregate;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.functions.builtin.cast.CastFuncBuilder;
import io.jimdb.core.types.Types;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public abstract class BaseAggregateExpr {

  private String name;
  private Expression[] args;
  private SQLType type;
  private AggregateType aggType;

  public BaseAggregateExpr(Session session, String name, Expression[] args) {
    this.name = name;
    this.args = args;
    this.aggType = AggregateType.valueOf(this.name);

    buildRetType(session);

//    if (this.getArgs()[0].getExprType() == COLUMN && this.type != this.args[0].getResultType()) {
//      this.getArgs()[0] = (CastFuncBuilder.build(null, this.type, this.getArgs()[0]));
//    }
  }

  public BaseAggregateExpr(Session session, AggregateType aggType, Expression[] args) {
    this.name = aggType.getName();
    this.args = args;
    this.aggType = aggType;

    buildRetType(session);
  }

  private void buildRetType(Session session) {
    switch (aggType) {
      case COUNT:
        SQLType.Builder builder = SQLType.newBuilder();
        builder.setType(DataType.BigInt)
                .setPrecision(21)
                .setBinary(true);
        this.type = builder.build();
        break;

      case SUM:
        buildRetTypeOnSum();
        break;

      case AVG:
        buildRetTypeOnAvg();
        break;

      case DISTINCT:
      case MAX:
      case MIN:
        buildRetTypeOnMaxMin(session);
        break;

      default:
        break;
    }
  }

  private void buildRetTypeOnSum() {
    DataType dataType = this.args[0].getResultType().getType();
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setBinary(true);

    switch (dataType) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
        builder.setType(DataType.BigInt);
//                .setPrecision(Types.MAX_DEC_WIDTH)
//                .setScale(0);
        this.type = builder.build();
        break;

      case Decimal:
        int scale = this.args[0].getResultType().getScale();
        if (scale < 0 || scale > Types.MAX_DEC_SCALE) {
          scale = Types.MAX_DEC_SCALE;
        }
        builder.setType(DataType.Decimal)
                .setPrecision(Types.MAX_DEC_WIDTH)
                .setScale(scale);
        this.type = builder.build();
        break;

      case Float:
      case Double:
        builder.setType(DataType.Double)
                .setPrecision(Types.MAX_REAL_WIDTH)
                .setScale(this.args[0].getResultType().getScale());
        this.type = builder.build();
        break;

      default:
        builder.setType(DataType.Double)
                .setPrecision(Types.MAX_REAL_WIDTH)
                .setScale(Types.UNDEFINE_WIDTH);
        this.type = builder.build();
        break;
    }
    this.type.toBuilder().setBinary(true);
  }

  private void buildRetTypeOnAvg() {
    DataType dataType = this.args[0].getResultType().getType();
    SQLType.Builder builder = SQLType.newBuilder();
    builder.setBinary(true);

    switch (dataType) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
//        builder.setType(DataType.BigInt);
//        this.type = builder.build();
//        break;
      case Decimal:
        int scale = this.args[0].getResultType().getScale();
        if (scale < 0) {
          scale = Types.MAX_DEC_SCALE;
        } else {
          scale = Math.min(scale + 4, Types.MAX_DEC_SCALE);
        }

        builder.setType(DataType.Decimal)
                .setPrecision(Types.MAX_DEC_WIDTH)
                .setScale(scale);
        this.type = builder.build();
        break;

      case Float:
      case Double:
        builder.setType(DataType.Double)
                .setPrecision(Types.MAX_REAL_WIDTH)
                .setScale(this.args[0].getResultType().getScale());
        this.type = builder.build();
        break;

      default:
        builder.setType(DataType.Double)
                .setPrecision(Types.MAX_REAL_WIDTH)
                .setScale(Types.UNDEFINE_WIDTH);
        this.type = builder.build();
        break;
    }
  }

  private void buildRetTypeOnMaxMin(Session session) {
    if (this.args[0].getExprType() == ExpressionType.FUNC
            && this.args[0].getResultType().getType() == Basepb.DataType.Float) {
      SQLType.Builder builder = SQLType.newBuilder();
      builder.setType(DataType.Double)
              .setPrecision(Types.MAX_REAL_WIDTH)
              .setScale(Types.UNDEFINE_WIDTH);
      this.args[0] = CastFuncBuilder.build(session, builder.build(), this.args[0]);
    }

    SQLType.Builder builder = this.args[0].getResultType().toBuilder();
    // TODO: && DataType.Bit
    if (aggType == AggregateType.MAX || aggType == AggregateType.MIN) {
      // TODO: clone
      builder.setNotNull(true);

      // TODO: enum, set
    }
    this.type = builder.build();

//    this.type = this.args[0].getResultType();
//    this.type.setNotNull(true);

//    // TODO: && DataType.Bit
//    if (aggType == AggregateType.MAX || aggType == AggregateType.MIN) {
//      // TODO: clone
//      this.type = this.args[0].getResultType();
//      this.type.setNotNull(true);
//
//      // TODO: enum, set
//    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AggregateExpr)) {
      return false;
    }
    BaseAggregateExpr other = (BaseAggregateExpr) obj;

    if (!other.getName().equals(this.name) || other.getArgs().length != this.args.length) {
      return false;
    }

    for (int i = 0; i < args.length; i++) {
      if (!this.args[i].equals(other.getArgs()[i])) {
        return false;
      }
    }
    return true;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Expression[] getArgs() {
    return args;
  }

  public void setArgs(Expression[] args) {
    this.args = args;
  }

  public SQLType getType() {
    return type;
  }

  public void setType(SQLType type) {
    this.type = type;
  }

  public AggregateType getAggType() {
    return aggType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    for (int i = 0; i < args.length; i++) {
      result += prime * args[i].hashCode();
    }
    return prime * result + ((name == null) ? 0 : name.hashCode());
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder(this.name);
    stringBuilder.append('(');
    for (Expression arg : this.args) {
      stringBuilder.append(arg).append(',');
    }

    stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(')');
    return stringBuilder.toString();
  }
}
