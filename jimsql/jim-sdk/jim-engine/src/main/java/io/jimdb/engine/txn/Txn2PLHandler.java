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

import static io.jimdb.engine.txn.PrepareHandler.PREPARE_PRIMARY_FUNC;

import java.util.List;
import java.util.Map;

import io.jimdb.engine.StoreCtx;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.engine.txn.TxnHandler.CommitSubscriber;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * 2PL:
 * prepare and commit
 *
 */
public final class Txn2PLHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Txn2PLHandler.class);

  private static final DecideHandler.DecidePrimaryFunc DECIDE_PRIMARY_FUNC = Txn2PLHandler::decidePrimary;
  private static final PrepareHandler.PrepareSecondaryFunc PREPARE_SECONDARY_FUNC = Txn2PLHandler::prepareSecondary;

  /**
   * commit
   *
   * @param config TODO
   * @param context TODO
   * @return TODO
   */
  public static Flux<ExecResult> commit(TxnConfig config, StoreCtx context) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("start to commit txn {}.", config.getTxnId());
    }
    return Flux.create(sink ->
            sink.onRequest(r -> preparePrimaryIntents(sink, config, context)));
  }

  //prepare primary intents
  public static void preparePrimaryIntents(FluxSink sink, TxnConfig config, StoreCtx context) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(" start to prepare txn {} primary.", config.getTxnId());
    }

    PREPARE_PRIMARY_FUNC.apply(context, config).onErrorResume(PrepareHandler.getErrHandler(context, PREPARE_PRIMARY_FUNC, config))
            .subscribe(new CommitSubscriber<>(rs -> prepareSecondaryIntents(sink, config, context, config.getSecIntents()), err -> sink.error(err)));
  }

  //prepare secondary intents
  public static void prepareSecondaryIntents(FluxSink sink, TxnConfig config, StoreCtx context, List<Txn.TxnIntent> secIntents) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(" start to prepare txn {} secondary.", config.getTxnId());
    }
    prepareSecondary(context, config, secIntents).subscribe(
            new CommitSubscriber<>(rs -> decidePrimaryIntents(sink, config, context), err -> sink.error(err)));
  }

  //decide primary intents
  public static void decidePrimaryIntents(FluxSink sink, TxnConfig config, StoreCtx context) {
    Txn.TxnStatus txnStatus = Txn.TxnStatus.COMMITTED;
    CommitSubscriber subscriber = new CommitSubscriber<>(rs -> sink.next(rs), err -> sink.error(err));
    DECIDE_PRIMARY_FUNC.apply(context, config, txnStatus).onErrorResume(
            DecideHandler.getErrHandler(context, DECIDE_PRIMARY_FUNC, config, txnStatus)).subscribe(subscriber);
  }

  public static Flux<ExecResult> prepareSecondary(StoreCtx context, TxnConfig config, List<Txn.TxnIntent> intentList) {

    if (intentList == null || intentList.isEmpty()) {
      return Flux.just(AckExecResult.getInstance());
    }

    Flux<ExecResult> flux = null;

    Map<RangeInfo, List<Txn.TxnIntent>> intentGroupMap;
    try {
      intentGroupMap = context.getRoutePolicy().regroupByRoute(intentList, intent -> intent.getKey().toByteArray());
    } catch (Throwable e) {
      LOGGER.warn("txn {} prepare Secondary {} error:{}", config.getTxnId(), intentList, e);
      return PrepareHandler.getErrHandler(context, PREPARE_SECONDARY_FUNC, config, intentList, e);
    }

    for (Map.Entry<RangeInfo, List<Txn.TxnIntent>> entry : intentGroupMap.entrySet()) {
      List<Txn.TxnIntent> groupList = entry.getValue();
      if (groupList == null || groupList.isEmpty()) {
        continue;
      }
      //OnErrorResume takes effect at each child flux,
      Flux<ExecResult> child = PrepareHandler.txnPrepareSecondary(context, config, groupList, entry.getKey());
      child = child.onErrorResume(PrepareHandler.getErrHandler(context, PREPARE_SECONDARY_FUNC, config, groupList));
      if (flux == null) {
        flux = child;
      } else {
        flux = flux.zipWith(child, (f1, f2) -> f1);
      }
    }
    return flux;
  }

  public static Flux<ExecResult> decidePrimary(StoreCtx context, TxnConfig config, Txn.TxnStatus status) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("start to decide txn {} primary", config.getTxnId());
    }
    RequestContext requestContext = DecideHandler.buildPrimaryDecideReqCtx(config, context, status);
    return DecideHandler.txnDecidePrimary(requestContext, context.getSender())
            .map(r -> AckExecResult.getInstance());
  }

  /**
   * rollback
   *
   * @param context TODO
   * @return TODO
   */
  public static Flux<ExecResult> rollback(TxnConfig config, StoreCtx context) {
    return Flux.create(sink ->
            sink.onRequest(r -> RecoverHandler.recoverFromPrimary(sink, config, context)));
  }
}

