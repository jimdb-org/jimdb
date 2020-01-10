/*
 * Copyright 2019 The Chubao Authors.
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
package io.jimdb.sql.smith.scalar;

import static io.jimdb.sql.smith.Random.rand6;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.expression.ExpressionType;
import io.jimdb.model.Column;
import io.jimdb.pb.Basepb;
import io.jimdb.sql.smith.Random;
import io.jimdb.sql.smith.Smither;
import io.jimdb.sql.smith.WeightedSampler;
import io.jimdb.sql.smith.agg.AggregateFunction;
import io.jimdb.sql.smith.cxt.Context;
import io.jimdb.sql.smith.cxt.Scope;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "DM_DEFAULT_ENCODING", "MDM_STRING_BYTES_ENCODING", "PSC_PRESIZE_COLLECTIONS",
        "UWF_UNWRITTEN_FIELD", "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", "MS_FINAL_PKGPROTECT",
        "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY" })
public class Scalar {

  protected MakeScalarFunction[] scalarFunctions;
  protected WeightedSampler scalarWeights;

  protected MakeScalarFunction[] boolFunctions;
  protected WeightedSampler boolWeights;

  protected AggregateFunction[] aggFunctions;
  protected WeightedSampler aggWeights;

  public Scalar() {

    scalarFunctions = new MakeScalarFunction[]{
            new MakeBinOpFunction(this, 10)
    };

    boolFunctions = new MakeScalarFunction[]{
            new MakeCompareFunction(this, 10)
    };

    int[] swt = new int[scalarFunctions.length];
    for (int i = 0; i < scalarFunctions.length; i++) {
      swt[i] = scalarFunctions[i].getWeight();
    }
    scalarWeights = new WeightedSampler(swt, Random.rand100());

    int[] bwt = new int[boolFunctions.length];
    for (int i = 0; i < boolFunctions.length; i++) {
      bwt[i] = boolFunctions[i].getWeight();
    }
    boolWeights = new WeightedSampler(bwt, Random.rand100());
  }

  protected static SQLBinaryOperator[] compareOps = { SQLBinaryOperator.GreaterThan, SQLBinaryOperator
          .GreaterThanOrEqual, SQLBinaryOperator.LessThanOrEqual, SQLBinaryOperator.LessThanOrEqualOrGreaterThan,
          SQLBinaryOperator.LessThanOrGreater };

  protected static SQLBinaryOperator[] binOps = { SQLBinaryOperator.Add, SQLBinaryOperator
          .Subtract, SQLBinaryOperator.Multiply, SQLBinaryOperator.Divide,
          SQLBinaryOperator.Modulus };

  public SQLExpr makeScalar(Scope scope, Column[] columns, Basepb.DataType dataType) {
    return makeScalarContext(scope, new Context(), dataType, columns);
  }

  public SQLExpr makeBoolExpr(Scope scope, Column[] columns) {
    return makeScalarSample(scope, new Context(), boolFunctions, boolWeights, columns, Basepb.DataType.TinyInt);
  }

  public SQLExpr makeScalarContext(Scope scope, Context cxt, Basepb.DataType dataType, Column[] columns) {
    return makeScalarSample(scope, cxt, scalarFunctions, scalarWeights, columns, dataType);
  }

  public SQLExpr makeScalarSample(Scope scope, Context context, MakeScalarFunction[] funtion, WeightedSampler weight,
                                  Column[] columns, Basepb.DataType dataTypes) {
    if (context.getType() == ExpressionType.FUNC) {
      //TODO make function
      return makeFunction(scope, dataTypes, columns);
    }

    if (scope.canRecurse()) {
      return funtion[weight.getNext()].makeSQLExpr(scope, dataTypes, columns);
    }

    if (Random.coin()) {
      //TODO make col ref
      return makeColRef(scope, dataTypes, columns);
    }

    return makeConstExpr(scope, columns);
  }

  public SQLExpr makeColRef(Scope scope, Basepb.DataType dataTypes, Column[] columns) {
    return getColRef(scope, dataTypes, columns);
  }

  public SQLExpr getColRef(Scope scope, Basepb.DataType dataTypes, Column[] columns) {
    Column column = columns[Random.getRandomInt(columns.length)];
    SQLIdentifierExpr expr = new SQLIdentifierExpr();
    expr.setName(column.getName());
    return expr;
  }
  @SuppressFBWarnings("NP_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD")
  public SQLExpr makeFunction(Scope scope, Basepb.DataType dataTypes, Column[] columns) {
    return this.aggFunctions[aggWeights.getNext()].makeSQLExpr(scope, dataTypes, columns);
  }

  public SQLExpr makeConstExpr(Scope scope, Column[] columns) {
    return getRandValuableExpr(scope.getSmither());
  }

  public SQLValuableExpr getRandValuableExpr(Smither smither) {

    Basepb.DataType randDataType = getRandDataType();
    switch (randDataType) {
      case Int:
        return new SQLIntegerExpr(smither.getRandomInt(Integer.MAX_VALUE));
      case Varchar:
        byte[] p = Random.generate(5);
        return new SQLCharExpr(new String(p));
      case Double:
        return new SQLNumberExpr(Random.getRandomDouble());
      case Float:
        return new SQLNumberExpr(Random.getRandomFloat());
      default:
        byte[] def = Random.generate(5);
        return new SQLCharExpr(new String(def));
    }
  }

  public static Basepb.DataType getRandDataType() {
    return Basepb.DataType.forNumber(Random.getRandomInt(20));
  }

  public static SQLBinaryOperator randCompareOps() {
    return compareOps[Random.getRandomInt(compareOps.length)];
  }

  public static SQLBinaryOperator randBinOps() {
    return binOps[Random.getRandomInt(binOps.length)];
  }

  public static Basepb.DataType[] makeDesiredTypes() {
    List<Basepb.DataType> types = new ArrayList<>();
    types.add(getRandDataType());
    while (rand6() < 2) {
      types.add(getRandDataType());
    }
    return types.toArray(new Basepb.DataType[0]);
  }
}
