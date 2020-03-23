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
package io.jimdb.sql.optimizer.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ExpressionType;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.aggregate.AggregateExpr;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Processorpb;
import io.jimdb.sql.operator.Aggregation;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.Limit;
import io.jimdb.sql.operator.Order;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Selection;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.TopN;
import io.jimdb.sql.optimizer.OperatorVisitor;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * PushDownBuilder gets the processor list from Logical Operator
 */
@SuppressFBWarnings("PSC_PRESIZE_COLLECTIONS")
public class PushDownBuilder {
  private static PBConvertVisitor visitor = new PBConvertVisitor();

  public static List<Processorpb.Processor.Builder> constructProcessors(RelOperator operator) {
    List<Processorpb.Processor.Builder> processors = new ArrayList<>();
    getPushDownProcessors(operator, processors);
    return processors;
  }

  private static void getPushDownProcessors(@Nonnull RelOperator relOperator, List<Processorpb.Processor.Builder> processors) {
    Objects.requireNonNull(relOperator);
    if (relOperator.hasChildren()) {
      for (RelOperator plan : relOperator.getChildren()) {
        getPushDownProcessors(plan, processors);
      }
    }
    processors.add(relOperator.acceptVisitor(visitor));
  }

  /**
   * Convert pushed-down operator to processor(PB)
   */
  public static class PBConvertVisitor extends OperatorVisitor<Processorpb.Processor.Builder> {

    @Override
    public Processorpb.Processor.Builder visitOperator(Aggregation operator) {
      Processorpb.Aggregation.Builder aggregationBuilder = Processorpb.Aggregation.newBuilder()
              .addAllFunc(aggregateExprsToPB(operator.getAggregateExprs()))
              .addAllGroupBy(exprsToPB(operator.getGroupByExprs()));
      Processorpb.Processor.Builder processor = Processorpb.Processor.newBuilder();
      if (operator.getAggOpType() == Aggregation.AggOpType.HashAgg) {
        processor.setType(Processorpb.ProcessorType.AGGREGATION_TYPE);
        processor.setAggregation(aggregationBuilder.build());
      } else {
        processor.setType(Processorpb.ProcessorType.STREAM_AGGREGATION_TYPE);
        processor.setStreamAggregation(aggregationBuilder.build());
      }
      return processor;
    }

    private List<Exprpb.Expr> aggregateExprsToPB(AggregateExpr[] aggregateExprs) {
      List<Exprpb.Expr> list = new ArrayList<>();
      for (AggregateExpr aggregateExpr : aggregateExprs) {
        list.add(aggregateExprToPB(aggregateExpr));
      }
      return list;
    }

    private Exprpb.Expr aggregateExprToPB(AggregateExpr aggregateExpr) {
      List<Exprpb.Expr> exprs = exprsToPB(aggregateExpr.getArgs());
      Exprpb.ExprType exprType;
      switch (aggregateExpr.getAggType()) {
        case COUNT:
          exprType = Exprpb.ExprType.Count;
          break;
        case SUM:
          exprType = Exprpb.ExprType.Sum;
          break;
        case DISTINCT:
          exprType = Exprpb.ExprType.Distinct;
          break;
        case MAX:
          exprType = Exprpb.ExprType.Max;
          break;
        case MIN:
          exprType = Exprpb.ExprType.Min;
          break;
        case AVG:
          exprType = Exprpb.ExprType.Avg;
          break;
        default:
          exprType = null;
          break;
      }
      return exprType != null ? Exprpb.Expr.newBuilder().setExprType(exprType).addAllChild(exprs).build() : null;
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(Projection operator) {
      List<Exprpb.Expr> exprs = exprsToPB(operator.getExpressions());
      Processorpb.Projection projection = Processorpb.Projection.newBuilder().addAllColumns(exprs).build();
      return Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.PROJECTION_TYPE)
              .setProjection(projection);
    }

    public List<Exprpb.Expr> exprsToPB(Expression[] expressions) {
      List<Exprpb.Expr> exprs = new ArrayList<>();
      for (Expression expression : expressions) {
        Exprpb.Expr expr = exprToPB(expression);
        exprs.add(expr);
      }
      return exprs;
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(Limit operator) {
      Processorpb.Limit limit = Processorpb.Limit.newBuilder()
              .setOffset(operator.getOffset())
              .setCount(operator.getCount())
              .build();
      return Processorpb.Processor.newBuilder().setType(Processorpb.ProcessorType.LIMIT_TYPE).setLimit(limit);
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(Order operator) {
      Processorpb.Ordering ordering = getOrdering(operator.getOrderExpressions());
      return Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.ORDER_BY_TYPE)
              .setOrdering(ordering);
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(Selection operator) {
      List<Exprpb.Expr> exprs = new ArrayList<>();
      for (Expression expression : operator.getConditions()) {
        Exprpb.Expr expr = exprToPB(expression);
        exprs.add(expr);
      }
      Processorpb.Selection selection = Processorpb.Selection.newBuilder().addAllFilter(exprs).build();
      return Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.SELECTION_TYPE)
              .setSelection(selection);
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(TableSource tableSource) {
//      if (tableSource.getColumns() == null || tableSource.getColumns().length == 0) {
//        return null;
//      }
      Processorpb.TableRead tableRead = Processorpb.TableRead.newBuilder()
              .addAllColumns(getAllColumns(tableSource.getColumns()))
              .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
              .setDesc(false)
              .build();
      return Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.TABLE_READ_TYPE)
              .setTableRead(tableRead);
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(IndexSource indexSource) {
      Processorpb.IndexRead indexRead = Processorpb.IndexRead.newBuilder()
              .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
              .addAllColumns(getAllColumns(indexSource.getSchema().getColumns()))
              .setUnique(indexSource.getKeyValueRange().getIndex().isUnique())
              .setDesc(false)
              .build();
      return Processorpb.Processor.newBuilder()
              .setType(Processorpb.ProcessorType.INDEX_READ_TYPE)
              .setIndexRead(indexRead);
    }

    @Override
    public Processorpb.Processor.Builder visitOperator(TopN topn) {
      Processorpb.Ordering ordering = getOrdering(topn.getCount(), topn.getOrderExpressions());
      return Processorpb.Processor.newBuilder().setType(Processorpb.ProcessorType.ORDER_BY_TYPE).setOrdering(ordering);
    }

    @Override
    public Processorpb.Processor.Builder visitOperatorByDefault(RelOperator operator) {
      return null;
    }

    public Exprpb.Expr exprToPB(Expression expr) {

      ExpressionType type = expr.getExprType();
      switch (type) {
        case CONST:
          return constToPBExpr(expr);
        case COLUMN:
          return columnToPBExpr((ColumnExpr) expr);
        case FUNC:
          return funcToPBExpr((FuncExpr) expr);
      }
      return null;
    }

    private Exprpb.Expr constToPBExpr(Expression expression) {
      Value value = expression.exec(ValueAccessor.EMPTY);
      Exprpb.ExprType type;
      ByteString v = null;

      switch (value.getType()) {
        case UNSIGNEDLONG:
          type = Exprpb.ExprType.Const_UInt;
          break;
        case STRING:
        case BINARY:
          type = Exprpb.ExprType.Const_Bytes;
          break;
        case DOUBLE:
          type = Exprpb.ExprType.Const_Double;
          break;
        case DECIMAL:
          type = Exprpb.ExprType.Const_Decimal;
          break;
        case TIME:
          type = Exprpb.ExprType.Const_Time;
          break;
        case DATE:
          type = Exprpb.ExprType.Const_Date;
          v = ByteString.copyFromUtf8(((DateValue) value).formatToString());
          break;
        default:
          type = Exprpb.ExprType.Const_Int;
      }

      if (v == null) {
        v = ByteString.copyFromUtf8(value.getString());
      }

      return Exprpb.Expr.newBuilder()
              .setExprType(type)
              .setValue(v)
              .build();
    }

    private Exprpb.Expr columnToPBExpr(ColumnExpr columnExpr) {
      return Exprpb.Expr.newBuilder()
              .setExprType(Exprpb.ExprType.Column)
              .setColumn(getColumnInfo(columnExpr))
              .build();
    }

    private Exprpb.Expr funcToPBExpr(FuncExpr funcExpr) {
      Expression[] args = funcExpr.getArgs();
      Exprpb.Expr[] children = new Exprpb.Expr[args.length];
      for (int i = 0; i < args.length; i++) {
        Exprpb.Expr expr = exprToPB(args[i]);
        children[i] = expr;
      }
      Exprpb.ExprType exprType = funcExpr.getFunc().getCode();
      return Exprpb.Expr.newBuilder().setExprType(exprType)
              .addAllChild(Arrays.asList(children))
              .build();
    }

    private Processorpb.Ordering getOrdering(Order.OrderExpression[] orderExpressions) {
      List<Processorpb.OrderByColumn> columns = getOrderByColumns(orderExpressions);
      return Processorpb.Ordering.newBuilder().addAllColumns(columns).build();
    }

    private Processorpb.Ordering getOrdering(long count, Order.OrderExpression[] orderExpressions) {
      List<Processorpb.OrderByColumn> columns = getOrderByColumns(orderExpressions);
      return Processorpb.Ordering.newBuilder().setCount(count).addAllColumns(columns).build();
    }

    private List<Processorpb.OrderByColumn> getOrderByColumns(Order.OrderExpression[] orderExpressions) {
      List<Processorpb.OrderByColumn> columns = new ArrayList<>();
      for (Order.OrderExpression orderExpression : orderExpressions) {
        Processorpb.OrderByColumn orderByColumn = Processorpb.OrderByColumn.newBuilder()
                .setExpr(exprToPB(orderExpression.getExpression()))
                .setAsc(orderExpression.getOrderType())
                .build();
        columns.add(orderByColumn);
      }
      return columns;
    }

    private List<Exprpb.ColumnInfo> getAllColumns(Column[] columns) {
      List<Exprpb.ColumnInfo> columnInfoList = new ArrayList<>(columns.length);
      for (Column column : columns) {
        columnInfoList.add(Exprpb.ColumnInfo.newBuilder()
                .setId(column.getId().intValue())
                .setTyp(column.getType().getType())
                .setUnsigned(column.getType().getUnsigned()).build());
      }
      return columnInfoList;
    }

    private List<Exprpb.ColumnInfo> getAllColumns(List<ColumnExpr> columnExprs) {
      List<Exprpb.ColumnInfo> columnInfoList = new ArrayList<>();
      for (ColumnExpr column : columnExprs) {
        columnInfoList.add(getColumnInfo(column));
      }
      return columnInfoList;
    }

    private Exprpb.ColumnInfo getColumnInfo(ColumnExpr columnExpr) {
      return Exprpb.ColumnInfo.newBuilder()
              .setId(Math.toIntExact(columnExpr.getId()))
              .setUnsigned(false)
              .setTyp(columnExpr.getResultType().getType())
              .build();
    }

  }
}
