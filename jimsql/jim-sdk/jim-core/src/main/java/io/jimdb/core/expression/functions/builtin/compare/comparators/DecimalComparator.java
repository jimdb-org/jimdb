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
package io.jimdb.core.expression.functions.builtin.compare.comparators;

import io.jimdb.core.Session;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.LongValue;

/**
 * @version V1.0
 */
public class DecimalComparator extends ExprComparator {
  static final DecimalComparator INSTANCE = new DecimalComparator();

  @Override
  public LongValue compare(Session session, Expression expr1, ValueAccessor row1, Expression expr2,
                           ValueAccessor row2, boolean nullComp) {
    DecimalValue value1 = expr1.execDecimal(session, row1);
    DecimalValue value2 = expr2.execDecimal(session, row2);
    if (value1 == null || value2 == null) {
      return nullComp ? compareNull(value1, value2) : null;
    }
    return LongValue.getInstance(value1.compareTo(session, value2));
  }
}
