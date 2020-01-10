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
package io.jimdb.core.expression;

import io.jimdb.core.values.NullValue;
import io.jimdb.core.values.Value;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

/**
 * Value accessor for expression evaluation.
 *
 * @version V1.0
 */
public interface ValueAccessor {
  ValueAccessor EMPTY = new ValueAccessor() {
    @Override
    public int size() {
      return 0;
    }

    @Override
    public Value get(int index) {
      return NullValue.getInstance();
    }

    @Override
    public void set(int index, Value value) {
      throw DBException.get(ErrorModule.EXPR, ErrorCode.ER_NOT_SUPPORTED_YET, "ValueAccessor set value");
    }
  };

  int size();

  Value get(int index);

  void set(int index, Value value);
}
