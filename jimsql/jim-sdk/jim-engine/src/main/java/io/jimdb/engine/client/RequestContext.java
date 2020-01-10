/*
 * Copyright 2019 The JimDB Authors.
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
package io.jimdb.engine.client;

import java.time.Instant;

import io.jimdb.engine.StoreCtx;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutePolicy;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Api.RangeRequest.ReqCase;
import io.jimdb.common.utils.os.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.NettyByteString;

/**
 * @version V1.0
 */
public final class RequestContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContext.class);

  private ByteString key;

  private Message.Builder reqBuilder;

  private ReqCase cmdType;

  private RangeInfo rangeInfo;

  private StoreCtx storeCtx;

  public RequestContext(StoreCtx storeCtx, ByteString key, Message.Builder builder, ReqCase cmdType) {
    this(storeCtx, key, null, builder, cmdType);
    this.rangeInfo = storeCtx.getRoutePolicy().getRangeInfoByKey(key.toByteArray());
  }

  public RequestContext(StoreCtx storeCtx, ByteString key, RangeInfo rangeInfo, Message.Builder builder, ReqCase cmdType) {
    this.storeCtx = storeCtx;
    this.key = key;
    this.rangeInfo = rangeInfo;
    this.reqBuilder = builder;
    this.cmdType = cmdType;
  }

  public Table getTable() {
    return this.storeCtx.getTable();
  }

  public void setTable(Table table) {
    this.storeCtx.setTable(table);
  }

  public RoutePolicy getRoutePolicy() {
    return this.storeCtx.getRoutePolicy();
  }

  public void setRoutePolicy(RoutePolicy routePolicy) {
    this.storeCtx.setRoutePolicy(routePolicy);
  }

  public Message.Builder getReqBuilder() {
    return this.reqBuilder;
  }

  public ByteString getKey() {
    return this.key;
  }

  public ReqCase getCmdType() {
    return this.cmdType;
  }

  public boolean isTimeout() {
    return SystemClock.currentTimeStamp().isAfter(this.storeCtx.getTimeout());
  }

  public long retryDelay() {
    return this.storeCtx.retryDelay();
  }

  public RangeInfo locateKey() {
    return this.storeCtx.getRoutePolicy().getRangeInfoByKey(NettyByteString.asByteArray(key));
  }

  public long getClusterId() {
    return this.storeCtx.getRoutePolicy().getClusterId();
  }

  public Instant getInstant() {
    return this.storeCtx.getTimeout();
  }

  public long getCtxId() {
    return this.storeCtx.getCxtId();
  }

  public void refreshRangeInfo() {
    this.rangeInfo = this.locateKey();
  }

  public RangeInfo getRangeInfo() {
    return rangeInfo;
  }
}
