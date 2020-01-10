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

import io.jimdb.pb.Basepb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public enum AggregateType {
  COUNT_ALL("CONUT_ALL", 0),
  COUNT("COUNT", 1),
  SUM("SUM", 2),
  AVG("AVG", 3),
  DISTINCT("DISTINCT", 4),
  MAX("MAX", 5),
  MIN("MIN", 6),
  GROUP_CONTACT("GROUP_CONCAT", 7),
  BIT_OR("BIT_OR", 8),
  BIT_XOR("BIT_XOR", 9),
  BIT_AND("BIT_AND", 10),
  UNKNOWN("UNKNOWN", 999);

  private String name;
  private int value;

  AggregateType(String name, int value) {
    this.name = name;
    this.value = value;
  }

  @SuppressFBWarnings({ "UP_UNUSED_PARAMETER" })
  public static Basepb.DataType typeInfer(AggregateType type) {
    switch (type) {
      case COUNT:
      case SUM:
      default:
        return Basepb.DataType.BigInt;
    }
  }

  public String getName() {
    return name;
  }

  public int getValue() {
    return value;
  }

}
