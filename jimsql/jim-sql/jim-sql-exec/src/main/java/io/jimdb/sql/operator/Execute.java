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
package io.jimdb.sql.operator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ExpressionUtil;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.sql.optimizer.physical.NFDetacher;
import io.jimdb.sql.optimizer.physical.RangeBuilder;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
@Deprecated
public final class Execute extends Operator {

  @Override
  public OperatorType getOperatorType() {
    return null;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    return null;
  }

  @SuppressFBWarnings("ITC_INHERITANCE_TYPE_CHECKING")
  public static void rebuildRange(Session session, Operator operator) {
    //todo visitor
    if (operator instanceof TableSource) {
      final TableSource tableSource = (TableSource) operator;
      Column[] columns = tableSource.getColumns();
      if (columns != null) {
        Arrays.stream(columns).filter(Column::isPrimary).findFirst().ifPresent(pkCol -> {
          final ColumnExpr pkColExpr = ExpressionUtil.idxCol2ColExpr(pkCol, tableSource.getSchema().getColumns());
          List<ValueRange> valueRanges;
          if (pkColExpr != null) {
            valueRanges = RangeBuilder.buildPKRange(session, tableSource.getAccessConditions(), pkCol.getType());
          } else {
            valueRanges = RangeBuilder.buildFullIntegerRange(true);
          }

          Arrays.stream(tableSource.getTable().getReadableIndices()).filter(Index::isPrimary).findFirst().ifPresent(index ->
                  tableSource.setKeyValueRange(new KeyValueRange(index, Lists.newArrayList(pkColExpr), valueRanges)));
        });
      }
    } else if (operator instanceof IndexSource) {
      IndexSource indexSource = (IndexSource) operator;
      List<ValueRange> valueRanges = rebuildRange4IndexSource(session, indexSource);
      indexSource.getKeyValueRange().setValueRanges(valueRanges);
    } else if (operator instanceof IndexLookup) {
      IndexLookup indexLookup = (IndexLookup) operator;
      IndexSource indexSource = indexLookup.getIndexSource();
      List<ValueRange> valueRanges = rebuildRange4IndexSource(session, indexSource);
      indexSource.getKeyValueRange().setValueRanges(valueRanges);
    } else if (operator instanceof RelOperator) {
      RelOperator relOperator = (RelOperator) operator;
      if (relOperator.hasChildren()) {
        for (RelOperator child : relOperator.getChildren()) {
          rebuildRange(session, child);
        }
      }
    } else if (operator instanceof Insert) {
      Insert insert = (Insert) operator;
      RelOperator select = insert.getSelect();
      if (select != null) {
        rebuildRange(session, select);
      }
    } else if (operator instanceof Update) {
      Update update = (Update) operator;
      RelOperator select = update.getSelect();
      if (select != null) {
        rebuildRange(session, select);
      }
    } else if (operator instanceof Delete) {
      Delete delete = (Delete) operator;
      RelOperator select = delete.getSelect();
      if (select != null) {
        rebuildRange(session, select);
      }
    }
  }

  private static List<ValueRange> rebuildRange4IndexSource(Session session, IndexSource indexSource) {
    Index index = indexSource.getKeyValueRange().getIndex();
    if (index == null) {
      return Collections.emptyList();
    }

    // find all the columnExpr corresponding to the index
    HashSet<Integer> ids = Arrays.stream(index.getColumns())
            .map(Column::getId).collect(Collectors.toCollection(HashSet::new));

    List<ColumnExpr> columnExprs = indexSource.getSchema().getColumns().stream()
            .filter(columnExpr -> ids.contains(columnExpr.getId()))
            .collect(Collectors.toList());

    return NFDetacher.buildRangeFromDetachedIndexConditions(session, indexSource.getAccessConditions(), columnExprs);
  }


}
