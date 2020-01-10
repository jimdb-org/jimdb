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

import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.values.Value;

/**
 * @version V1.0
 */
public interface Cell {
  void setValue(Value value);

  Value getValue();

  Cell plus(Session session, Value src);

  Cell plus(Session session, Cell src);

  default Set getDistinctSet() {
    return null;
  }

  default boolean hasDistinct() {
    return false;
  }
}
