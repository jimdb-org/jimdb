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

import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.sql.operator.Delete;
import io.jimdb.sql.operator.IndexLookup;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.Insert;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.Update;
import io.jimdb.sql.optimizer.OperatorVisitor;

/**
 * RangeRebuildVisitor  Rebuild value ranges for prepare.
 *
 * @version V1.0
 */
public class RangeRebuildVisitor extends OperatorVisitor<Operator> {

  private Session session;

  public RangeRebuildVisitor(Session session) {
    this.session = session;
  }

  private static List<ValueRange> rebuildRange4Index(Session session, List<ColumnExpr> idxCols, List<Expression> accessConditions) {
    return NFDetacher.buildRangeFromDetachedIndexConditions(session, accessConditions, idxCols);
  }

  @Override
  public Operator visitOperator(TableSource tableSource) {
    tableSource
            .getKeyValueRange()
            .setValueRanges(rebuildRange4Index(session, tableSource.getKeyValueRange().getIndexColumns(),
                    tableSource.getAccessConditions()));
    return tableSource;
  }

  @Override
  public Operator visitOperator(IndexSource indexSource) {
    indexSource
            .getKeyValueRange()
            .setValueRanges(rebuildRange4Index(session, indexSource.getKeyValueRange().getIndexColumns(),
                    indexSource.getAccessConditions()));
    return indexSource;
  }

  @Override
  public Operator visitOperator(IndexLookup indexLookup) {
    IndexSource indexSource = indexLookup.getIndexSource();
    indexSource
            .getKeyValueRange()
            .setValueRanges(rebuildRange4Index(session, indexSource.getKeyValueRange().getIndexColumns(),
                    indexSource.getAccessConditions()));
    return indexLookup;
  }

  @Override
  public Operator visitOperator(Insert insert) {
    RelOperator relOperator = insert.getSelect();
    if (relOperator != null) {
      relOperator.acceptVisitor(this);
    }
    return relOperator;
  }

  @Override
  public Operator visitOperator(Update update) {
    RelOperator relOperator = update.getSelect();
    if (relOperator != null) {
      relOperator.acceptVisitor(this);
    }
    return relOperator;
  }

  @Override
  public Operator visitOperator(Delete delete) {
    RelOperator relOperator = delete.getSelect();
    if (relOperator != null) {
      relOperator.acceptVisitor(this);
    }
    return relOperator;
  }

  @Override
  public Operator visitOperatorByDefault(RelOperator relOperator) {
    if (relOperator.hasChildren()) {
      for (RelOperator child : relOperator.getChildren()) {
        child.acceptVisitor(this);
      }
    }
    return relOperator;
  }
}
