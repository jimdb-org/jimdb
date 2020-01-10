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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "FCBL_FIELD_COULD_BE_LOCAL", "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", "EI_EXPOSE_REP" })
public final class AggregateExpr extends BaseAggregateExpr {

  private boolean hasDistinct;
  private AggFunctionMode aggFunctionMode = AggFunctionMode.COMPLETE_MODE;

  public AggregateExpr(Session session, String name, Expression[] args, boolean hasDistinct) {
    super(session, name, args);
    this.hasDistinct = hasDistinct;
  }

  public AggregateExpr(Session session, AggregateType aggType, Expression[] args, boolean hasDistinct) {
    super(session, aggType, args);
    this.hasDistinct = hasDistinct;
  }

  public boolean isHasDistinct() {
    return hasDistinct;
  }

  public AggFunctionMode getAggFunctionMode() {
    return aggFunctionMode;
  }

  public void setAggFunctionMode(AggFunctionMode aggFunctionMode) {
    this.aggFunctionMode = aggFunctionMode;
  }

  /**
   * @version V1.0
   */
  public enum AggFunctionMode {

    COMPLETE_MODE, FINAL_MODE, PARTIAL1_MODE, PARTIAL2_MODE, DEDUP_MODE

  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AggregateExpr)) {
      return false;
    }
    AggregateExpr otherAgg = (AggregateExpr) obj;
    if (this.hasDistinct != otherAgg.isHasDistinct()) {
      return false;
    }
    return super.equals(otherAgg);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (hasDistinct ? 1 : 0);
    return 31 * result + aggFunctionMode.hashCode();
  }
}
