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

import java.util.function.Consumer;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.engine.Dispatcher;
import io.jimdb.engine.ShardSender;
import io.jimdb.engine.StoreCtx;
import io.jimdb.meta.Router;
import io.jimdb.pb.Txn;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Transaction class that implements functions defined in the Transaction interface
 */
public class TransactionImpl implements Transaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionImpl.class);
  private final Session session;
  private volatile Txn.TxnStatus status = Txn.TxnStatus.TXN_INIT;
  private TxnConfig config;
  private Dispatcher dispatcher;
  private Router router;
  private ShardSender shardSender;

  // FIXME can the session be null here? if not, we can add @NonNull tag here and simplify the code for initializing ttl
  public TransactionImpl(Session session, Router router, Dispatcher dispatcher, ShardSender shardSender) {
    this.session = session;
    this.router = router;
    this.dispatcher = dispatcher;
    this.shardSender = shardSender;

    int ttl = 0;
    if (session != null) {
      ttl = Integer.parseInt(session.getVarContext().getGlobalVariable("innodb_lock_wait_timeout"));
    }
    this.config = new TxnConfig(ttl);
  }

  @Override
  public Flux<ExecResult> commit() throws BaseException {
    if (!isPending()) {
      changeTxnStatus(Txn.TxnStatus.COMMITTED);
      close();
      return Flux.just(AckExecResult.getInstance());
    }

    changeTxnStatus(Txn.TxnStatus.COMMITTED);
    TxnConfig curConfig = this.close();
    StoreCtx storeCtx = StoreCtx.buildCtx(session, curConfig.getTable(), router);

    if (curConfig.isLocal()) {
      return Txn1PLHandler.commit(shardSender, curConfig, storeCtx).map(r -> {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("1PL: txn {} commit success", curConfig.getTxnId());
        }
        return r;
      });
    }

    curConfig.sortIntents();
    return Txn2PLHandler.commit(shardSender, curConfig, storeCtx)
               .map(r -> {
                 if (LOGGER.isInfoEnabled()) {
                   LOGGER.info("2PL: txn {} commit success", curConfig.getTxnId());
                 }
                 this.addTask(new CommitSuccessTask(shardSender, curConfig, storeCtx));
                 return r;
               }).doOnError(e -> {
                 //rollback
                 LOGGER.error("2PL: txn {} commit error, start to rollback, err:", curConfig.getTxnId(), e);
                 Txn2PLHandler.rollback(shardSender, curConfig, storeCtx).subscribe(new TxnCallbackSubscriber(curConfig.getTxnId()));
               });
  }

  @Override
  public Flux<ExecResult> rollback() throws BaseException {
    if (!isPending()) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_ERROR_DURING_ROLLBACK, "transaction has no DML statement"
                                                                                        + " or has been rolled back");
    }

    changeTxnStatus(Txn.TxnStatus.ABORTED);
    TxnConfig curConfig = this.close();
    StoreCtx storeCtx = StoreCtx.buildCtx(session, curConfig.getTable(), router);

    if (curConfig.isLocal()) {
      return Txn1PLHandler.rollback(shardSender, curConfig, storeCtx);
    }

    curConfig.sortIntents();
    return Txn2PLHandler.rollback(shardSender, curConfig, storeCtx);
  }

  @Override
  public void addIntent(KvPair kvPair, Txn.OpType opType, boolean check, long version, Table table) {
    // TODO do we need to check kvPair.getValue() != null
    Txn.TxnIntent.Builder intent = Txn.TxnIntent.newBuilder().setTyp(opType).setKey(kvPair.getKey());

    // prevent throwing new NullPointerException()
    if (kvPair.getValue() != null) {
      intent.setValue(kvPair.getValue());
    }
    intent.setCheckUnique(check).setExpectedVer(version);

    this.config.addIntent(intent.build(), table);
  }

  @Override
  public String getTxnId() {
    return this.config.getTxnId();
  }

  @Override
  public boolean isPending() {
    return !this.config.emptyIntents();
  }

  private void changeTxnStatus(Txn.TxnStatus newStatus) throws BaseException {
    if (newStatus != Txn.TxnStatus.TXN_INIT
            && newStatus != Txn.TxnStatus.COMMITTED
            && newStatus != Txn.TxnStatus.ABORTED) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TXN_STATE_INVALID, newStatus.name());
    }
    Txn.TxnStatus oldStatus = this.status;
    this.status = newStatus;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("change transaction status from {} to {}", oldStatus, newStatus);
    }
  }

  //release resource
  private TxnConfig close() {
    TxnConfig oldConfig = this.config;
    this.config = new TxnConfig(oldConfig.getLockTTl());
    this.changeTxnStatus(Txn.TxnStatus.TXN_INIT);
    return oldConfig;
  }

  private void addTask(TxnAsyncTask runnable) {
    if (runnable != null) {
      this.dispatcher.enqueue(runnable);
    }
  }

  /**
   * Txn commit subscriber
   *
   * @param <T> TODO
   */
  static final class CommitSubscriber<T> extends BaseSubscriber<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CommitSubscriber.class);

    private final Consumer<T> success;
    private final Consumer<Throwable> error;
    private boolean ok = false;

    CommitSubscriber(final Consumer<T> success, final Consumer<Throwable> error) {
      this.success = success;
      this.error = error;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(1);
    }

    @Override
    protected void hookOnNext(final T value) {
      success.accept(value);
      ok = true;
      dispose();
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      error.accept(throwable);
    }

    @Override
    protected void hookOnCancel() {
      if (ok) {
        return;
      }
      error.accept(DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SYSTEM_CANCELLED));
    }
  }

  //  private static <T> Flux<T> noShardFlux(StoreCtx context, Flux<T> publisher, JimException exception) {
//    if (exception.getCode() != ErrorCode.ER_SHARD_NOT_EXIST) {
//      return null;
//    }
//
////    //maybe: table no exist, or range no exist
//    RangeRouteException routeException = (RangeRouteException) exception;
//    RangeInfo rangeInfo = context.getRoutePolicy().getRangeInfoByKeyFromCache(routeException.key);
//    if (rangeInfo != null && !StringUtils.isBlank(rangeInfo.getLeaderAddr())) {
//      return publisher.onErrorResume(getErrHandler(context, publisher));
//    }
//    return getRouteFlux(context, routeException.key)
//            .flatMap(flag -> publisher.onErrorResume(getErrHandler(context, publisher)));
//  }

  //  /**
//   * @param <T>
//   * @version V1.0
//   */
//  static final class ErrorHandlerCallback<T> implements Callback {
//    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlerCallback.class);
//
//    private final Flux<T> publisher;
//    private final CommitSubscriber<T> subscriber;
//    private final FluxSink sink;
////    FunctionalInterface
//
//    private final TxnContext context;
//
//    ErrorHandlerCallback(final Flux<T> publisher, final CommitSubscriber<T> subscriber, FluxSink sink, TxnContext context) {
//
//      this.publisher = publisher;
//      this.subscriber = subscriber;
//      this.sink = sink;
//      this.context = context;
//    }
//
//    public void success(boolean value) {
////      LOG.warn("txn {} ErrorHandlerCallback success", this.context.getConfig().getTxnId());
//      publisher.onErrorResume(getErrHandler(context, publisher)).subscribe(this.subscriber);
//    }
//
//    public void failed(final Throwable throwable) {
////      LOG.warn("txn {} ErrorHandlerCallback failed {}", this.context.getConfig().getTxnId(), throwable);
//      sink.error(throwable);
//    }
//  }
}
