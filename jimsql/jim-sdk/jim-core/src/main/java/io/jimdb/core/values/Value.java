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
package io.jimdb.core.values;

import io.jimdb.core.Session;
import io.jimdb.common.config.SystemProperties;
import io.jimdb.core.types.ValueType;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @param <T>
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public abstract class Value<T extends Value> {
  protected static final int BYTE_CACHE_THRESHOLD = SystemProperties.getByteCacheThreshold();
  protected static final int VALUE_CACHE_SIZE = SystemProperties.getValueCacheSize();
  protected static final int VALUE_CACHE_MASK = SystemProperties.getValueCacheSize() - 1;

  public static final Value MAX_VALUE = new NullValue();
  public static final Value MIN_VALUE = new NullValue();
  public static final Value NULL_VALUE = new NullValue(); // marker of null value in a null range

  public abstract ValueType getType();

  public abstract String getString();

  public abstract byte[] toByteArray();

  public boolean isNull() {
    return false;
  }

  public boolean isMax() {
    return this == MAX_VALUE;
  }

  public boolean isMin() {
    return this == MIN_VALUE;
  }

  @Override
  public String toString() {
    return getString();
  }

  public int signum() {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name(), "signum");
  }

  protected int compareToSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "compare");
  }

  protected Value plusSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "plus");
  }

  protected Value subtractSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "subtract");
  }

  protected Value multiplySafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "multiply");
  }

  protected Value divideSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "divide");
  }

  protected Value divSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "div");
  }

  protected Value modSafe(final T v2) {
    throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, getType().name() + "," + v2.getType().name(), "mod");
  }

  public final int compareTo(Session session, Value v2) {
    if (this == v2) {
      return 0;
    }

    if (this.isMin() || v2.isMax() || (this.isNull() && !this.isMax() && !v2.isMin())) {
      return -1;
    }

    if (this.isMax() || v2.isMin() || (v2.isNull() && !v2.isMax())) {
      return 1;
    }

    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();
    if (type1 != type2) {
      if (this.needUnsignedType(type1, type2)) {
        v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
        v2 = ValueConvertor.convertType(session, v2, ValueType.UNSIGNEDLONG, null);
      } else {
        switch (type2) {
          case DECIMAL:
            v1 = ValueConvertor.convertType(session, v1, ValueType.DECIMAL, null);
            break;
          case DOUBLE:
            v1 = ValueConvertor.convertType(session, v1, ValueType.DOUBLE, null);
            break;
          case STRING:
            v1 = ValueConvertor.convertType(session, v1, ValueType.STRING, null);
            break;
          case LONG:
            v1 = ValueConvertor.convertType(session, v1, ValueType.LONG, null);
            break;
          case UNSIGNEDLONG:
            v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
            break;
          case DATE:
            v1 = ValueConvertor.convertType(session, v1, ValueType.DATE, null);
            break;
          case TIME:
            v1 = ValueConvertor.convertType(session, v1, ValueType.TIME, null);
            break;
          case NULL:
            // nothing to do
            break;
          default:
            throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, type1.name() + "," + type2.name(), "compare");
        }
      }
    }
    return v1.compareToSafe(v2);
  }

  public final Value plus(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();
    if (type1 != type2) {
      if (this.needUnsignedType(type1, type2)) {
        v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
        v2 = ValueConvertor.convertType(session, v2, ValueType.UNSIGNEDLONG, null);
      } else {
        v2 = ValueConvertor.convertType(session, v2, this.getType(), null); // try v2 to v1 type
      }
    }
    return v1.plusSafe(v2);
  }

  public final Value subtract(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();
    if (type1 != type2) {
      if (this.needUnsignedType(type1, type2)) {
        v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
        v2 = ValueConvertor.convertType(session, v2, ValueType.UNSIGNEDLONG, null);
      } else {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, type1.name() + "," + type2.name(), "subtract");
      }
    }
    return v1.subtractSafe(v2);
  }

  public final Value multiply(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();
    if (type1 != type2) {
      if (this.needUnsignedType(type1, type2)) {
        v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
        v2 = ValueConvertor.convertType(session, v2, ValueType.UNSIGNEDLONG, null);
      } else {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, type1.name() + "," + type2.name(), "multiply");
      }
    }
    return v1.multiplySafe(v2);
  }

  public final Value divide(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();

    if (type1 != ValueType.DECIMAL) {
      v1 = ValueConvertor.convertType(session, v1, ValueType.DOUBLE, null);
    }

    if (type2 != ValueType.DECIMAL) {
      v2 = ValueConvertor.convertType(session, v2, ValueType.DOUBLE, null);
    }

    return v1.divideSafe(v2);
  }

  public final Value div(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();

    if (type1 != ValueType.DECIMAL) {
      v1 = ValueConvertor.convertType(session, v1, ValueType.DOUBLE, null);
    }

    if (type2 != ValueType.DECIMAL) {
      v2 = ValueConvertor.convertType(session, v2, ValueType.DOUBLE, null);
    }

    return v1.divSafe(v2);
  }

  public final Value mod(Session session, Value v2) {
    Value v1 = this;
    ValueType type1 = v1.getType();
    ValueType type2 = v2.getType();
    if (type1 != type2) {
      if (this.needUnsignedType(type1, type2)) {
        v1 = ValueConvertor.convertType(session, v1, ValueType.UNSIGNEDLONG, null);
        v2 = ValueConvertor.convertType(session, v2, ValueType.UNSIGNEDLONG, null);
      } else {
        throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_SYSTEM_VALUE_OPERATION_ERROR, type1.name() + "," + type2.name(), "mod");
      }
    }
    return v1.modSafe(v2);
  }

  private boolean needUnsignedType(ValueType t1, ValueType t2) {
    return (t1 == ValueType.LONG && t2 == ValueType.UNSIGNEDLONG) || (t1 == ValueType.UNSIGNEDLONG && t2 == ValueType.LONG);
  }
}
