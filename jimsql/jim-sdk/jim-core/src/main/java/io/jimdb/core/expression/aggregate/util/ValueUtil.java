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
package io.jimdb.core.expression.aggregate.util;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.ValueConvertor;
import io.jimdb.pb.Basepb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "NP_NULL_PARAM_DEREF_NONVIRTUAL" })
public class ValueUtil {

  /**
   * make value to real type value
   *
   * @param session
   * @param value
   * @param type
   * @return
   */
  public static Value cast(Session session, Value value, Basepb.DataType type) {
    switch (type) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
      case Bit:
        return ValueConvertor.convertToLong(session, value, null);
      case Float:
      case Double:
        return ValueConvertor.convertToDouble(session, value, null);
      case Varchar:
      case Char:
      case NChar:
      case Text:
        return ValueConvertor.convertToString(session, value, null);
      case Binary:
      case VarBinary:
        return ValueConvertor.convertToBinary(session, value, null);
      case Date:
      case TimeStamp:
      case DateTime:
        return ValueConvertor.convertToDate(session, value, null);
      case Time:
        return ValueConvertor.convertToTime(session, value, null);
      case Year:
        return ValueConvertor.convertToYear(session, value, null);
      case Decimal:
        return ValueConvertor.convertToDecimal(session, value, null);
      case Json:
        return ValueConvertor.convertToJson(session, value, null);
      case Null:
        return NullValue.getInstance();
      default:
        throw new RuntimeException("unknow type:" + type);
    }
  }

  public static Value exec(Session session, Expression expre, ValueAccessor accessor, Basepb.DataType type) {
    switch (type) {
      case TinyInt:
      case SmallInt:
      case MediumInt:
      case Int:
      case BigInt:
      case Bit:
        return expre.execLong(session, accessor);
      case Float:
      case Double:
        return expre.execDouble(session, accessor);
      case Varchar:
      case Char:
      case NChar:
      case Text:
      case Binary:
      case VarBinary:
        return expre.execString(session, accessor);
      case Date:
      case TimeStamp:
      case DateTime:
        return expre.execDate(session, accessor);
      case Time:
        return expre.execTime(session, accessor);
      case Year:
        return expre.execYear(session, accessor);
      case Decimal:
        return expre.execDecimal(session, accessor);
      case Json:
        return expre.execJson(session, accessor);
      case Null:
        return NullValue.getInstance();
      default:
        throw new RuntimeException("unknow type:" + type);
    }
  }
}
