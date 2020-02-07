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
package io.jimdb.sql.analyzer;

import static io.jimdb.core.types.Types.MAX_SIGNEDLONG;
import static io.jimdb.core.types.Types.MAX_UNSIGNEDLONG;
import static io.jimdb.core.types.Types.MIN_DEC_SIGNEDLONG;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb.SQLType;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLRealExpr;
import com.alibaba.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public final class AnalyzerUtil {
  private AnalyzerUtil() {
  }

  public static Table resolveTable(Session session, SQLTableSource tableSource) throws JimException {
    if (tableSource instanceof SQLExprTableSource) {
      SQLExprTableSource exprTable = (SQLExprTableSource) tableSource;
      String tblName = exprTable.getName().getSimpleName();
      String dbName = exprTable.getSchema();
      if (StringUtils.isBlank(dbName)) {
        dbName = session.getVarContext().getDefaultCatalog();
      }

      return session.getTxnContext().getMetaData().getTable(dbName, tblName);
    }

    throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "multiple-table");
  }

  public static long resolveLimt(SQLExpr expr) {
    if (expr == null) {
      return 0;
    }

    LongValue offsetValue = resolveLongFromExpr(expr);
    if (offsetValue == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_WRONG_ARGUMENTS, "LIMIT");
    }
    return offsetValue.getValue();
  }

  public static LongValue resolveLongFromExpr(final SQLExpr expr) {
    if (expr instanceof SQLIntegerExpr) {
      return LongValue.getInstance(((SQLIntegerExpr) expr).getNumber().longValue());
    }

    if (expr instanceof SQLCharExpr) {
      try {
        long l = Long.parseLong(((SQLCharExpr) expr).getText());
        return LongValue.getInstance(l);
      } catch (Exception ex) {
      }
    } else if (expr instanceof SQLNumberExpr) {
      return LongValue.getInstance(((SQLNumberExpr) expr).getNumber().longValue());
    } else if (expr instanceof SQLVariantRefExpr) {
      return LongValue.getInstance(0);
    }

    return null;
  }

  public static Value resolveValueExpr(SQLObject expr, ValueExpr valueExpr) {
    final Class clazz = expr.getClass();
    Value value;

    if (clazz == SQLIntegerExpr.class) {
      value = resolveIntegerValue(((SQLIntegerExpr) expr).getNumber(), valueExpr);
    } else if (clazz == SQLNumberExpr.class) {
      value = resolveIntegerValue(((SQLNumberExpr) expr).getNumber(), valueExpr);
    } else if (clazz == SQLRealExpr.class) {
      value = DoubleValue.getInstance(((SQLRealExpr) expr).getValue());
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Float));
      }
    } else if (clazz == SQLCharExpr.class) {
      value = StringValue.getInstance(((SQLCharExpr) expr).getText());
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Varchar));
      }
    } else if (clazz == SQLBooleanExpr.class) {
      value = LongValue.getInstance(((SQLBooleanExpr) expr).getBooleanValue() ? 1 : 0);
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.TinyInt));
      }
    } else if (clazz == SQLTimestampExpr.class) {
      value = DateValue.getInstance(((SQLTimestampExpr) expr).getValue(), DataType.TimeStamp);
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.TimeStamp));
      }
    } else if (clazz == SQLDateExpr.class) {
      value = DateValue.getInstance(((SQLDateExpr) expr).getValue(), DataType.Date);
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Date));
      }
    } else if (clazz == SQLHexExpr.class) {
      value = BinaryValue.getInstance(((SQLHexExpr) expr).getValue());
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Binary));
      }
    } else if (clazz == SQLBinaryExpr.class) {
      value = StringValue.getInstance(((SQLBinaryExpr) expr).getText());
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Varchar));
      }
    } else if (clazz == SQLNullExpr.class) {
      value = NullValue.getInstance();
      if (valueExpr != null) {
        valueExpr.setResultType(Types.buildSQLType(DataType.Null));
      }
    } else {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, String.format("value type %s", clazz.getName()));
    }

    if (valueExpr != null) {
      valueExpr.setValue(value);
    }
    return value;
  }

  private static Value resolveIntegerValue(Number number, ValueExpr valueExpr) {
    final Value value;
    final SQLType resultType;

    if (number instanceof BigInteger) {
      BigInteger bigInteger = (BigInteger) number;
      // convert over maxUnsignedLong or less minLong to bigDecimal
      if (bigInteger.compareTo(MAX_UNSIGNEDLONG) > 0 || bigInteger.compareTo(MIN_DEC_SIGNEDLONG.toBigInteger()) < 0) {
        BigDecimal dec = new BigDecimal(bigInteger, 4);
        value = DecimalValue.getInstance(dec, dec.precision(), dec.scale());
        resultType = Types.buildSQLType(DataType.Decimal);
      } else if (bigInteger.compareTo(MAX_SIGNEDLONG) > 0) {
        value = UnsignedLongValue.getInstance(bigInteger);
        resultType = Types.buildSQLType(DataType.BigInt, true);
      } else {
        value = LongValue.getInstance(bigInteger.longValue());
        resultType = Types.buildSQLType(DataType.BigInt);
      }
    } else if (number instanceof BigDecimal) {
      BigDecimal dec = (BigDecimal) number;
      value = DecimalValue.getInstance(dec, dec.precision(), dec.scale());
      resultType = Types.buildSQLType(DataType.Decimal);
    } else {
      value = LongValue.getInstance(number.longValue());
      resultType = Types.buildSQLType(DataType.BigInt);
    }

    if (valueExpr != null) {
      valueExpr.setResultType(resultType);
    }
    return value;
  }

  static int resolveFromSelectFields(List<SQLSelectItem> selectItems, SQLName sqlName, boolean ignoreAsName) {
    int index = -1;
    SQLExpr matchedExpr = null;
    for (int i = 0; i < selectItems.size(); i++) {
      SQLSelectItem sqlSelectItem = selectItems.get(i);

      if (sqlSelectItem instanceof OrderByAnalyzer.SQLSelectItemWrapper) {
        continue;
      }

      if (matchField(sqlSelectItem, sqlName, ignoreAsName)) {
        SQLExpr expr = sqlSelectItem.getExpr();
        if (!(expr instanceof SQLIdentifierExpr)) {
          return i;
        }

        if (matchedExpr == null) {
          matchedExpr = expr;
          index = i;
        } else if (!matchCol(matchedExpr, expr) && !matchCol(expr, matchedExpr)) {
          return -1;
        }
      }
    }

    return index;
  }

  static boolean matchCol(SQLExpr left, SQLExpr right) {

    boolean isLeftPro = left instanceof SQLPropertyExpr;
    boolean isRightPro = right instanceof SQLPropertyExpr;

    if (!isLeftPro) {
      SQLIdentifierExpr leftExpr = (SQLIdentifierExpr) left;
      if (!isRightPro) {
        SQLIdentifierExpr rightExpr = (SQLIdentifierExpr) right;
        return leftExpr.getName().equalsIgnoreCase(rightExpr.getName());
      }

      SQLPropertyExpr rightPro = (SQLPropertyExpr) right;
      return leftExpr.getName().equalsIgnoreCase(rightPro.getName());
    } else {
      if (isRightPro) {
        SQLPropertyExpr leftExpr = (SQLPropertyExpr) left;
        SQLPropertyExpr rightExpr = (SQLPropertyExpr) right;
        SQLExpr leftExprOwner = leftExpr.getOwner();

        if (leftExprOwner != null) {
          if (leftExprOwner instanceof SQLPropertyExpr) {

            SQLExpr rightExprOwner = rightExpr.getOwner();

            if (rightExprOwner == null || !(rightExprOwner instanceof SQLPropertyExpr)) {
              return false;
            }

            return false;
          } else {
            SQLExpr rightExprOwner = rightExpr.getOwner();
            SQLIdentifierExpr leftProOwner = (SQLIdentifierExpr) leftExprOwner;
            if (rightExprOwner instanceof SQLIdentifierExpr) {
              if (((SQLIdentifierExpr) rightExprOwner).getName().equalsIgnoreCase(leftProOwner.getName())) {
                return leftExpr.getName().equalsIgnoreCase(rightExpr.getName());
              }
            } else {
              if (((SQLPropertyExpr) rightExprOwner).getName().equalsIgnoreCase(leftProOwner.getName())) {
                return leftExpr.getName().equalsIgnoreCase(rightExpr.getName());
              }
            }
          }
        }
      } else {
        SQLPropertyExpr leftExpr = (SQLPropertyExpr) left;
        SQLIdentifierExpr rightExpr = (SQLIdentifierExpr) right;
        return leftExpr.getName().equalsIgnoreCase(rightExpr.getName());
      }
      return false;
    }
  }

  @SuppressFBWarnings("BL_BURYING_LOGIC")
  private static boolean matchField(SQLSelectItem selectItem, SQLName col, boolean ignoreAsName) {
    if (!(col instanceof SQLPropertyExpr)) {
      if (StringUtils.isBlank(selectItem.getAlias()) || ignoreAsName) {
        SQLExpr expr = selectItem.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
          return ((SQLIdentifierExpr) expr).getLowerName().equalsIgnoreCase(col.getSimpleName());
        } else if (expr instanceof SQLBinaryOpExpr) {
          return expr.toString().equalsIgnoreCase(col.getSimpleName());
        } else {
          //other case use resolveFromSchema to item
          return false;
        }
      }
      return selectItem.getAlias().equalsIgnoreCase(col.getSimpleName());
    }
    return false;
  }
}
