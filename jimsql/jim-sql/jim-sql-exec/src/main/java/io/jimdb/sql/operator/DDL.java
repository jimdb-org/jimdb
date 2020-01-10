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

import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.pb.Ddlpb.AlterTableInfo;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Metapb.CatalogInfo;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.sql.ddl.DDLExecutor;

import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

/**
 * @version V1.0
 */
public final class DDL extends Operator {
  private final OpType type;
  private final Object stmt;
  private final boolean isNotExists;

  public DDL(OpType type, Object stmt, boolean isNotExists) {
    this.type = type;
    this.stmt = stmt;
    this.isNotExists = isNotExists;
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.DDL;
  }

  @Override
  public Flux<ExecResult> execute(Session session) throws JimException {
    Flux<Boolean> result;
    switch (type) {
      case CreateCatalog:
        result = DDLExecutor.createCatalog((CatalogInfo) stmt)
                .onErrorReturn(err -> {
                  if (err instanceof JimException) {
                    return isNotExists && ((JimException) err).getCode() == ErrorCode.ER_DB_CREATE_EXISTS;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case DropCatalog:
        result = DDLExecutor.dropCatalog((CatalogInfo) stmt)
                .onErrorReturn(err -> {
                  if (err instanceof JimException) {
                    return isNotExists && ((JimException) err).getCode() == ErrorCode.ER_BAD_DB_ERROR;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case CreateTable:
        result = DDLExecutor.createTable((TableInfo) stmt)
                .onErrorReturn(err -> {
                  if (err instanceof JimException) {
                    return isNotExists && ((JimException) err).getCode() == ErrorCode.ER_TABLE_EXISTS_ERROR;
                  }
                  return false;
                }, Boolean.TRUE);
        break;
      case DropTable:
        final List<AlterTableInfo> dropTables = (List<AlterTableInfo>) stmt;
        result = Flux.create(sink -> {
          final Throwable[] errs = new Throwable[1];
          final List<String> notExists = new ArrayList<>(dropTables.size());
          Flux.just(dropTables.toArray(new AlterTableInfo[0]))
                  .flatMap(table -> DDLExecutor.alterTable(table)
                          .onErrorReturn(err -> {
                            if (err instanceof JimException && ((JimException) err).getCode() == ErrorCode.ER_BAD_TABLE_ERROR) {
                              notExists.add(table.getTableName());
                              return true;
                            }

                            errs[0] = err;
                            return false;
                          }, Boolean.TRUE))
                  .subscribe(new BaseSubscriber<Boolean>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                      request(1);
                    }

                    @Override
                    protected void hookOnNext(final Boolean value) {
                      request(1);
                    }

                    @Override
                    protected void hookFinally(SignalType type) {
                      if (errs[0] == null && !isNotExists && notExists.size() > 0) {
                        errs[0] = DBException.get(ErrorModule.DDL, ErrorCode.ER_BAD_TABLE_ERROR, notExists.toString());
                      }
                      if (errs[0] != null) {
                        sink.error(errs[0]);
                      } else {
                        sink.next(Boolean.TRUE);
                      }
                    }
                  });
        });
        break;
      case AlterTable:
        final List<AlterTableInfo> alterItems = (List<AlterTableInfo>) stmt;
        result = Flux.create(sink -> {
          Flux.just(alterItems.toArray(new AlterTableInfo[0]))
                  .flatMap(e -> {
                    OpType opType = e.getType();
                    switch (opType) {
                      case AddIndex:
                        return DDLExecutor.addIndex(e);
                      case DropIndex:
                      case AddColumn:
                      case DropColumn:
                      case RenameTable:
                      case RenameIndex:
                      case AlterAutoInitId:
                        return DDLExecutor.alterTable(e);
                      default:
                        return Flux.error(DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "DDLType(" + type.name() + ")"));
                    }
                  })
                  .subscribe(new BaseSubscriber<Boolean>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                      request(1);
                    }

                    @Override
                    protected void hookOnNext(final Boolean value) {
                      request(1);
                    }

                    @Override
                    protected void hookOnError(final Throwable throwable) {
                      sink.error(throwable);
                    }

                    @Override
                    protected void hookOnComplete() {
                      sink.next(Boolean.TRUE);
                    }
                  });
        });
        break;

      default:
        result = Flux.error(DBException.get(ErrorModule.DDL, ErrorCode.ER_NOT_SUPPORTED_YET, "DDLType(" + type.name() + ")"));
    }

    return result.map(rs -> AckExecResult.getInstance());
  }
}
