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

import java.util.List;

import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.pb.Txn;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * @version V1.0
 */
public abstract class TxnAsyncTask implements Runnable {
  protected TxnConfig config;
  protected StoreCtx context;

  TxnAsyncTask(TxnConfig config, StoreCtx context) {
    this.config = config;
    this.context = context;
  }
}

/**
 * @version V1.0
 */
final class TxnAsyncTaskSubscriber extends BaseSubscriber<Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TxnAsyncTaskSubscriber.class);

  private String txnId;

  TxnAsyncTaskSubscriber(String txnId) {
    this.txnId = txnId;
  }

  private String getTxnId() {
    return this.txnId == null ? "" : this.txnId;
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    request(1);
  }

  @Override
  protected void hookOnNext(Object value) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("txn {} decide secondary and clear up success", getTxnId());
    }

    dispose();
  }

  @Override
  protected void hookOnError(final Throwable throwable) {
    LOGGER.error("txn{} decide secondary or clear up failure, err: {}, next retry", getTxnId(), throwable);
  }
}

/**
 * @version V1.0
 */
final class TxnCallbackSubscriber extends BaseSubscriber<Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TxnCallbackSubscriber.class);

  private String txnId;

  TxnCallbackSubscriber(String txnId) {
    this.txnId = txnId;
  }

  private String getTxnId() {
    return this.txnId == null ? "" : this.txnId;
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    request(1);
  }

  @Override
  protected void hookOnNext(Object value) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("txn {} rollback success", getTxnId());
    }

    dispose();
  }

  @Override
  protected void hookOnError(final Throwable throwable) {
    LOGGER.error("txn{} rollback failure, err: {}, next retry", getTxnId(), throwable);
  }
}

/**
 * version 1.0
 */
class CommitSuccessTask extends TxnAsyncTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitSuccessTask.class);

  CommitSuccessTask(TxnConfig config, StoreCtx context) {
    super(config, context);
  }

  @Override
  public void run() {
    BaseSubscriber subscription = new TxnAsyncTaskSubscriber(config.getTxnId());

    List<Txn.TxnIntent> secIntents = config.getSecIntents();
    if (secIntents == null || secIntents.isEmpty()) {
      Flux.create(sink ->
              sink.onRequest(r -> clearUp(sink))).subscribe(subscription);
    } else {

      Flux.create(sink ->
              sink.onRequest(r -> decideSecondary(sink))).subscribe(subscription);
    }
  }

  //decide secondary intents: cannot rollback, if execute failure
  public void decideSecondary(FluxSink sink) {
    TxnHandler.decideSecondary(this.context, this.config.getTxnId(), this.config.getSecKeys(), Txn.TxnStatus.COMMITTED)
            .subscribe(
                    new TxnHandler.CommitSubscriber<>(flag -> {
                      if (!flag) {
                        sink.next(flag);
                      } else {
                        clearUp(sink);
                      }
                    }, err -> sink.error(err)));
  }

  public void clearUp(FluxSink sink) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("start to clear txn {}", this.config.getTxnId());
    }
    CleanUpFunc cleanUpFunc = (ctx, conf) -> cleanUp(ctx, conf);
    cleanUpFunc.apply(this.context, this.config).onErrorResume(
            TxnHandler.getErrHandler(this.context, cleanUpFunc, this.config)).subscribe(
            new TxnHandler.CommitSubscriber<>(rs -> sink.next(rs), err -> sink.error(err)));
  }

  private Flux<Txn.ClearupResponse> cleanUp(StoreCtx context, TxnConfig config) {
    RequestContext reqCtx = TxnHandler.buildClearUpReqCtx(config, context);
    return TxnHandler.clearUp(reqCtx, this.context.getSender());
  }
}
