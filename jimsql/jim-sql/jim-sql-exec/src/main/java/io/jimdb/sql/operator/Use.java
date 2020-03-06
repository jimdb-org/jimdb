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

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.Session;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.sql.ddl.DDLUtils;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Use extends Operator {
  private final String databases;

  public Use(String databases) {
    this.databases = databases;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    if (null == session.getTxnContext().getMetaData().getCatalog(DDLUtils.trimName(databases))) {
      throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_BAD_DB_ERROR, databases);
    } else {
      if (!PluginFactory.getPrivilegeEngine().catalogIsVisible(session.getUserInfo().getUser(), session.getUserInfo()
              .getHost(), DDLUtils.trimName(databases))) {
        throw DBException.get(ErrorModule.EXECUTOR, ErrorCode.ER_DBACCESS_DENIED_ERROR, session.getUserInfo().getUser
                (), session.getUserInfo().getHost(), databases);
      }
      session.getVarContext().setDefaultCatalog(DDLUtils.trimName(databases));
      return Flux.just(AckExecResult.getInstance());
    }
  }
}
