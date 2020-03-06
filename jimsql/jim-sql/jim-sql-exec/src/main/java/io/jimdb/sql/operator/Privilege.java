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

import io.jimdb.core.Session;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.PrivilegeOp;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public final class Privilege extends Operator {
  private final OpType type;
  private final PrivilegeOp stmt;
  private final PrivilegeEngine priEngine;

  public Privilege(OpType type, PrivilegeOp stmt) {
    this.type = type;
    this.stmt = stmt;
    this.priEngine = PluginFactory.getPrivilegeEngine();
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SIMPLE;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws BaseException {
    Flux<Boolean> result = null;
    switch (type) {
      case PriGrant:
        result = priEngine.grant(OpType.PriGrant, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_ILLEGAL_GRANT_FOR_TABLE;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriRevoke:
        result = priEngine.grant(OpType.PriRevoke, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_ILLEGAL_GRANT_FOR_TABLE;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriSetPassword:
        result = priEngine.grant(OpType.PriSetPassword, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_ILLEGAL_GRANT_FOR_TABLE;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriCreateUser:
        result = priEngine.grant(OpType.PriCreateUser, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_CANT_CREATE_USER_WITH_GRANT;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriUpdateUser:
        result = priEngine.grant(OpType.PriUpdateUser, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_ACCESS_DENIED_CHANGE_USER_ERROR;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriDropUser:
        result = priEngine.grant(OpType.PriDropUser, stmt)
                .onErrorReturn(err -> {
                  if (err instanceof BaseException) {
                    return ((BaseException) err).getCode() == ErrorCode.ER_DROP_USER;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case PriFlush:
        priEngine.flush();
        result = Flux.just(Boolean.TRUE);
        break;

      default:
        result = Flux.error(DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_NOT_SUPPORTED_YET, "PrivilegeType(" + type.name() + ")"));
    }

    return result.map(rs -> AckExecResult.getInstance());
  }
}
