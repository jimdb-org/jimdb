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
package io.jimdb.engine.txn;

import static io.jimdb.engine.txn.Preparer.PREPARE_PRIMARY_FUNC;

import java.util.function.Consumer;

import io.jimdb.engine.ShardSender;
import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.txn.TransactionImpl.CommitSubscriber;
import io.jimdb.core.model.result.ExecResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * 1PL: prepare
 *
 */
public final class Txn1PLHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(Txn1PLHandler.class);

  public static Flux<ExecResult> commit(ShardSender shardSender, TxnConfig config, StoreCtx context) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("start to commit txn {}.", config.getTxnId());
    }
    return Flux.create(sink ->
            sink.onRequest(r -> prepare(shardSender, sink, config, context)));
  }

  public static void prepare(ShardSender shardSender, FluxSink sink, TxnConfig config, StoreCtx context) {
    PREPARE_PRIMARY_FUNC.apply(shardSender, context, config).onErrorResume(
            Preparer.getErrHandler(shardSender, context, PREPARE_PRIMARY_FUNC, config)).subscribe(
            new CommitSubscriber<>((Consumer<ExecResult>) sink::next, sink::error));
  }

  public static Flux<ExecResult> rollback(ShardSender shardSender, TxnConfig config, StoreCtx context) {
    return Flux.create(sink ->
            sink.onRequest(r -> {
              Restorer.recoverFromPrimary(shardSender, sink, config, context);
            }));
  }
}
