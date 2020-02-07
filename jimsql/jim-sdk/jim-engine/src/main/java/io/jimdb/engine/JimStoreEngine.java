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
package io.jimdb.engine;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.engine.sender.DistSender;
import io.jimdb.engine.table.alloc.AutoIncrIDAllocator;
import io.jimdb.engine.table.alloc.IDAllocator;
import io.jimdb.engine.txn.JimTransaction;
import io.jimdb.meta.RouterManager;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Statspb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.NettyByteString;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * storage engine
 *
 * @version V1.0
 */
public final class JimStoreEngine implements Engine {
  private static final Logger LOGGER = LoggerFactory.getLogger(JimStoreEngine.class);

  //route
  private RouterManager routerManager;

  private DistSender distSender;

  private IDAllocator idAllocator;

  @Override
  public void init(JimConfig c) {
    this.idAllocator = new AutoIncrIDAllocator(PluginFactory.getMetaStore(), c.getRowIdStep());
    this.routerManager = new RouterManager(PluginFactory.getRouterStore(), c);
    this.distSender = new DistSender(c, this.routerManager);
    try {
      this.distSender.start();
    } catch (Exception e) {
      LOGGER.error("distSender start rpc error:{}", e.getMessage());
    }
  }

  @Override
  public Transaction beginTxn(Session session) {
    return new JimTransaction(session, routerManager, distSender, idAllocator);
  }

  @Override
  public Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws JimException {
    KvPair keyValue = new KvPair(NettyByteString.wrap(key), NettyByteString.wrap(value));
    return this.distSender.rawPut(StoreCtx.buildCtx(table, timeout, routerManager, distSender), keyValue);
  }

  @Override
  public Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws JimException {
    return this.distSender.rawGet(StoreCtx.buildCtx(table, timeout, routerManager, distSender), key);
  }

  @Override
  public Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws JimException {
    return this.distSender.rawDel(StoreCtx.buildCtx(table, timeout, routerManager, distSender), key);
  }

  @Override
  public Set<RangeInfo> getRanges(Table table) throws JimException {
    final int tableId = table.getId();
    final int dbId = table.getCatalog().getId();
    KvPair tableScope = Codec.encodeTableScope(tableId);
    RoutePolicy routePolicy = this.routerManager.getOrCreatePolicy(dbId, tableId);

    byte[] startKey = NettyByteString.asByteArray(tableScope.getKey());
    byte[] endKey = NettyByteString.asByteArray(tableScope.getValue());
    return routePolicy.getSerialRoute(startKey, endKey);
  }

  @Override
  public void close() {
    this.distSender.close();
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(
          Table table, Index index, Instant timeout, List<ValueRange> ranges, Statspb.IndexStatsRequest.Builder reqBuilder) {

    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges);
    Processorpb.KeyRange.Builder keyRangeBuilder = Processorpb.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sending request for analyzing index {} that has columns {}",
              index.getId(), Arrays.stream(index.getColumns()).map(Column::getId).collect(Collectors.toList()));
    }

    return this.distSender.analyzeIndex(StoreCtx.buildCtx(table, timeout, routerManager, distSender), reqBuilder);
  }

  @Override
  public Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(
          Table table, Column[] columns, Instant timeout, List<ValueRange> ranges, Statspb.ColumnsStatsRequest.Builder reqBuilder) {
    // set the range in request builder
    List<KvPair> kvPairs = Codec.encodeKeyRanges(table.getReadableIndices()[0], ranges); // table.getIndexes()[0] is the primary key index
    Processorpb.KeyRange.Builder keyRangeBuilder = Processorpb.KeyRange.newBuilder().setStartKey(kvPairs.get(0).getKey()).setEndKey(kvPairs.get(0).getValue());
    reqBuilder.setRange(keyRangeBuilder);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sending request for analyzing columns {}", Arrays.stream(columns).map(Column::getId).collect(Collectors.toList()));
    }

    return this.distSender.analyzeColumns(StoreCtx.buildCtx(table, timeout, routerManager, distSender), reqBuilder);
  }
}
