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
package io.jimdb.engine.client;

import java.time.Instant;

import io.jimdb.engine.StoreCtx;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.meta.route.RoutingPolicy;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Api.RangeRequest.ReqCase;

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

  private Table table;

  private RoutingPolicy routingPolicy;

  private StoreCtx storeCtx;

  private Instant timeOut;

  private long cxtId;

  public RequestContext(StoreCtx storeCtx, ByteString key, RangeInfo rangeInfo, Message.Builder builder, ReqCase cmdType) {
    this.storeCtx = storeCtx;
    this.key = key;
    this.rangeInfo = rangeInfo;
    this.reqBuilder = builder;
    this.cmdType = cmdType;

    // TODO not needed once we refactor the exception handling
    this.table = storeCtx.getTable();
    this.routingPolicy = storeCtx.getRoutingPolicy();
    this.timeOut = storeCtx.getTimeout();
    this.cxtId = storeCtx.getCxtId();

    if (rangeInfo == null) {
      refreshRangeInfo();
    }
  }

  public Table getTable() {
    return this.table;
  }

  public RoutingPolicy getRoutePolicy() {
    return this.routingPolicy;
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

  public long retryDelay() {
    return this.storeCtx.retryDelay();
  }

  public long getClusterId() {
    return this.routingPolicy.getClusterId();
  }

  public Instant getTimeOut() {
    return this.timeOut;
  }

  public long getCtxId() {
    return this.cxtId;
  }

  public void refreshRangeInfo() {
    this.rangeInfo = this.routingPolicy.getRangeInfoByKey(NettyByteString.asByteArray(key));
  }

  public RangeInfo getRangeInfo() {
    return rangeInfo;
  }
}



///*
// * Copyright 2019 The JIMDB Authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// * implied. See the License for the specific language governing
// * permissions and limitations under the License.
// */
//package io.jimdb.engine.client;
//
//import java.time.Instant;
//import java.util.TimeZone;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//
//import io.jimdb.common.utils.retry.RetryPolicy;
//import io.jimdb.core.Session;
//import io.jimdb.engine.Dispatcher;
//import io.jimdb.engine.StoreCtx;
//import io.jimdb.core.model.meta.RangeInfo;
//import io.jimdb.meta.Router;
//import io.jimdb.meta.route.RoutingPolicy;
//import io.jimdb.core.model.meta.Table;
//import io.jimdb.pb.Api.RangeRequest.ReqCase;
//import io.jimdb.common.utils.os.SystemClock;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.protobuf.ByteString;
//import com.google.protobuf.Message;
//import com.google.protobuf.NettyByteString;
//
///**
// * @version V1.0
// */
//public final class RequestContext {
//  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContext.class);
//
//  private ByteString key;
//
//  private Message.Builder reqBuilder;
//
//  private ReqCase cmdType;
//
//  private RangeInfo rangeInfo;
//
//  //private StoreCtx storeCtx;
//
//  private static final AtomicLong ID = new AtomicLong(0L);
//  private long cxtId;
//
//  private Table table;
//  private RoutingPolicy routingPolicy;
//  private Instant timeout;
//
//  private TimeZone timeZone = TimeZone.getDefault();
//
//  private RetryPolicy retryPolicy;
//  private AtomicInteger retry = new AtomicInteger(0);
//
//  private RequestContext(ByteString key, Message.Builder reqBuilder, ReqCase cmdType, RangeInfo rangeInfo, long cxtId,
//                         Table table, RoutingPolicy routingPolicy, Instant timeout) {
//    this.key = key;
//    this.reqBuilder = reqBuilder;
//    this.cmdType = cmdType;
//    this.rangeInfo = rangeInfo;
//    this.cxtId = ID.incrementAndGet();
//    this.table = table;
//    this.routingPolicy = routingPolicy;
//    this.timeout = timeout == null ? SystemClock.currentTimeStamp().plusSeconds(20) : timeout;
//    this.retryPolicy = new RetryPolicy.Builder().retryDelay(20).useExponentialBackOff(true).backOffMultiplier(1.35).build();
//  }
//
//  public RequestContext(StoreCtx storeCtx, ByteString key, Message.Builder builder, ReqCase cmdType) {
//    this(storeCtx, key, null, builder, cmdType);
//    this.rangeInfo = storeCtx.getRoutingPolicy().getRangeInfoByKey(key.toByteArray());
//  }
//
//  public RequestContext(StoreCtx storeCtx, ByteString key, RangeInfo rangeInfo, Message.Builder builder, ReqCase cmdType) {
//    this.storeCtx = storeCtx;
//    this.key = key;
//    this.rangeInfo = rangeInfo;
//    this.reqBuilder = builder;
//    this.cmdType = cmdType;
//  }
//
//
//
//  public Table getTable() {
//    return this.table;
//  }
//
//  public RoutingPolicy getRoutePolicy() {
//    return this.routingPolicy;
//  }
//
//  public Message.Builder getReqBuilder() {
//    return this.reqBuilder;
//  }
//
//  public ByteString getKey() {
//    return this.key;
//  }
//
//  public ReqCase getCmdType() {
//    return this.cmdType;
//  }
//
//  public boolean isTimeout() {
//    return SystemClock.currentTimeStamp().isAfter(this.timeout);
//  }
//
//  public boolean canRetryWithDelay() {
//    long delay = retryDelay();
//    if (delay < 0) {
//      return false;
//    }
//    try {
//      Thread.sleep(delay);
//    } catch (InterruptedException e) {
//      LOGGER.warn("store ctx retry sleep failure.");
//    }
//    return true;
//  }
//
//  public long retryDelay() {
//    this.retry.addAndGet(1);
//    return retryPolicy.getDelay(this.timeout, retry.get());
//  }
//
//  public RangeInfo locateKey() {
//    return this.routingPolicy.getRangeInfoByKey(NettyByteString.asByteArray(key));
//  }
//
//  public long getClusterId() {
//    return this.routingPolicy.getClusterId();
//  }
//
//  public Instant getInstant() {
//    return this.timeout;
//  }
//
//  public long getCtxId() {
//    return this.cxtId;
//  }
//
//  public void refreshRangeInfo() {
//    this.rangeInfo = this.locateKey();
//  }
//
//  public RangeInfo getRangeInfo() {
//    return rangeInfo;
//  }
//
//  public RequestContextBuilder newBuilder() {
//    return new RequestContextBuilder();
//  }
//
//  public static class RequestContextBuilder {
//
//    private ByteString key;
//
//    private Message.Builder messageBuilder;
//
//    private ReqCase cmdType;
//
//    private RangeInfo rangeInfo;
//
//    private long cxtId;
//
//    private Table table;
//    private RoutingPolicy routingPolicy;
//    private Instant timeout;
//
//    private RetryPolicy retryPolicy;
//
//    RequestContextBuilder() {
//      this.cxtId = ID.incrementAndGet();
//      this.retryPolicy = new RetryPolicy.Builder().retryDelay(20).useExponentialBackOff(true).backOffMultiplier(1.35)
//          .build();
//    }
//
//    public RequestContext build() {
//      return new RequestContext();
//    }
//
//    public ByteString getKey() {
//      return key;
//    }
//
//    public RequestContextBuilder setKey(ByteString key) {
//      this.key = key;
//      return this;
//    }
//
//    public Message.Builder getMessageBuilder() {
//      return messageBuilder;
//    }
//
//    public RequestContextBuilder setMessageBuilder(Message.Builder messageBuilder) {
//      this.messageBuilder = messageBuilder;
//      return this;
//    }
//
//    public ReqCase getCmdType() {
//      return cmdType;
//    }
//
//    public RequestContextBuilder setCmdType(ReqCase cmdType) {
//      this.cmdType = cmdType;
//      return this;
//    }
//
//    public RangeInfo getRangeInfo() {
//      return rangeInfo;
//    }
//
//    public long getCxtId() {
//      return cxtId;
//    }
//
//    public Table getTable() {
//      return table;
//    }
//
//    public RequestContextBuilder setTable(Table table) {
//      this.table = table;
//      return this;
//    }
//
//    public RoutingPolicy getRoutingPolicy() {
//      return routingPolicy;
//    }
//
//    public RequestContextBuilder setRoutingPolicy(Router router) {
//      this.routingPolicy = router.getOrCreatePolicy(table.getCatalog().getId(), table.getId());
//      this.rangeInfo = routingPolicy.getRangeInfoByKey(key.toByteArray());
//      return this;
//    }
//
//    public Instant getTimeout() {
//      return timeout;
//    }
//
//    public RequestContextBuilder setTimeout(Session session) {
//      this.timeout = session == null ? null : session.getStmtContext().getTimeout();;
//      return this;
//    }
//  }
//}
