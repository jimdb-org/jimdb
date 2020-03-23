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
package io.jimdb.test.mock.store;

import io.jimdb.common.exception.BaseException;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.pb.Txn;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class MockTransaction implements Transaction {

  @Override

  public Flux<ExecResult> commit() throws BaseException {
    return Flux.just(AckExecResult.getInstance());
  }

  @Override
  public Flux<ExecResult> rollback() throws BaseException {
    return Flux.just(AckExecResult.getInstance());
  }

  @Override
  public void addIntent(KvPair kvPair, Txn.OpType opType, boolean check, long version, Table table) {

  }

  @Override
  public String getTxnId() {
    return null;
  }

  @Override
  public boolean isPending() {
    return true;
  }
}
