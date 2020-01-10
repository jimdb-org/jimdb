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
package io.jimdb.sql.operator.show;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.Session;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.Value;

import org.apache.commons.lang3.StringUtils;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class ShowGrants extends RelOperator {
  private final String user;
  private final String host;
  private final PrivilegeEngine priEngine;

  public ShowGrants(String user, String host) {
    this.user = user;
    this.host = StringUtils.isBlank(host) ? "%" : host;
    this.priEngine = PluginFactory.getPrivilegeEngine();
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    UserInfo userInfo = new UserInfo(user, host);
    List<String> values = priEngine.showGrant(userInfo, user, host);
    List<RowValueAccessor> rows = new ArrayList<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      RowValueAccessor row = new RowValueAccessor(new Value[values.size()]);
      row.set(i, StringValue.getInstance(values.get(i)));
      rows.add(row);
    }
    return Flux.just(new QueryExecResult(schema.getColumns().toArray(new ColumnExpr[0]),
            rows.toArray(new ValueAccessor[0])));
  }
}
