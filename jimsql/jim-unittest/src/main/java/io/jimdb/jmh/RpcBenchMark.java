///*
// * Copyright 2019 The Chubao Authors.
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
//package io.jimdb.jmh;
//
//import java.time.Instant;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//
//import io.jimdb.Bootstraps;
//import io.jimdb.config.JimConfig;
//import io.jimdb.engine.EngineUtil;
//import io.jimdb.engine.JimStoreEngine;
//import io.jimdb.engine.KvPair;
//import io.jimdb.engine.client.JimCommand;
//import io.jimdb.engine.client.RequestContext;
//import io.jimdb.engine.codec.KVCodec;
//import io.jimdb.engine.sender.Util;
//import io.jimdb.meta.route.RoutePolicy;
//import io.jimdb.model.Table;
//import io.jimdb.pb.Api;
//import io.jimdb.plugin.MetaEngine;
//import io.jimdb.plugin.store.Engine;
//import io.jimdb.rpc.client.command.CommandCallback;
//
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Level;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.OutputTimeUnit;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.Setup;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.TearDown;
//import org.openjdk.jmh.runner.Runner;
//import org.openjdk.jmh.runner.RunnerException;
//import org.openjdk.jmh.runner.options.Options;
//import org.openjdk.jmh.runner.options.OptionsBuilder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @version V1.0
// */
////@BenchmarkMode(Mode.AverageTime) // 测试方法平均执行时间
//@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
//@OutputTimeUnit(TimeUnit.SECONDS)// 输出结果的时间粒度为秒
//@State(Scope.Benchmark) // 每个测试线程一个实例
//public class RpcBenchMark {
//  private static final Logger LOG = LoggerFactory.getLogger(EngineBenchMark.class);
//  private static final AtomicLong ID = new AtomicLong(0L);
//
//  KVCodec kvCodec;
//  MetaEngine meta;
//  JimStoreEngine storeEngine;
//  int type;
//  String catalog;
//  String tableName;
//  Table t;
//  RoutePolicy routePolicy;
//  KvPair kvPair;
//
//  public RpcBenchMark() {
//  }
//
//  @Setup(Level.Trial)
//  public void init() {
//    try {
//      JimConfig config = Bootstraps.init("jim.properties");
//      this.catalog = config.getString("catalog", "maggie");
//      this.tableName = config.getString("table.name", "maggie2");
//      this.type = config.getInt("test.type", 1);
//      this.meta = config.getMetaEngine();
//      Engine e = config.getStoreEngine();
//      if (!(e instanceof JimStoreEngine)) {
//        throw new IllegalArgumentException("engine error");
//      }
//      storeEngine = (JimStoreEngine) e;
//      this.kvCodec = storeEngine.getKvCodec();
//
//      this.t = this.meta.getMetaData().getTable(this.catalog, this.tableName);
//      this.routePolicy = storeEngine.getRouteManger().getOrCreatePolicy(t.getId());
//
//      long rowId = ID.incrementAndGet();
//      Object[] row = new Object[]{ rowId, String.format("%d", rowId), null };
//      this.kvPair = this.kvCodec.encodeRecordRow(t, t.getColumns(), row);
//    } catch (Throwable var2) {
//      var2.printStackTrace();
//    }
//  }
//
//  @Benchmark
//  public void testMethod() {
//    if (type == 1) {
//      testSingeRpcMethod();
//    } else if (type == 2) {
//      testRpcMethodNoCode();
//    } else {
//      testRpcMethod();
//    }
//  }
//
//  public void testSingeRpcMethod() {
//    RequestContext context = new RequestContext(t, this.routePolicy, kvPair.getKey(),
//            Util.buildRawPut(kvPair.getKey(), kvPair.getValue()), Api.RangeRequest.ReqCase.KV_PUT, EngineUtil.getTimeoutInstant());
//    CountDownLatch latch = new CountDownLatch(1);
//    CommandCallback<JimCommand> callback = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        latch.countDown();
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    storeEngine.getDistSender().getShardSender().sendCommand(context, callback);
//    try {
//      latch.await();
//    } catch (InterruptedException e) {
//      LOG.error("wait err ", e);
//    }
//  }
//
//  public void testRpcMethodNoCode() {
//    CountDownLatch latch = new CountDownLatch(1);
//    RequestContext context = new RequestContext(t, routePolicy, kvPair.getKey(),
//            Util.buildRawPut(kvPair.getKey(), kvPair.getValue()),
//            Api.RangeRequest.ReqCase.KV_PUT, EngineUtil.getTimeoutInstant());
//    CommandCallback<JimCommand> callback3 = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        latch.countDown();
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    CommandCallback<JimCommand> callback2 = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        storeEngine.getDistSender().getShardSender().sendCommand(context, callback3);
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    CommandCallback<JimCommand> callback = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        storeEngine.getDistSender().getShardSender().sendCommand(context, callback2);
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    storeEngine.getDistSender().getShardSender().sendCommand(context, callback);
//
//    try {
//      latch.await();
//    } catch (InterruptedException e) {
//      LOG.error("wait err ", e);
//    }
//  }
//
//  public void testRpcMethod() {
//    Object[][] rows = new Object[3][3];
//    for (int i = 0; i < 3; i++) {
//      long rowId = ID.incrementAndGet();
//      Object[] row = new Object[]{ rowId, String.format("%d", rowId), null };
//      row[2] = row[1];
//      rows[i] = row;
//    }
//
//    Instant timeout = EngineUtil.getTimeoutInstant(EngineUtil.RPC_TIMEOUT * 3);
//
//    CountDownLatch latch = new CountDownLatch(3);
//    CommandCallback<JimCommand> callback3 = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
////        try {
////          MessageHeader header = (MessageHeader) request.getHeader();
////          Message message = (Message) ConvertUtil.parseResponse(header.getFuncId(), response.getPayload(), "getResp");
////          LOG.warn("requestId {} success, response:{}", header.getRequestId(), message);
////        } catch (Throwable throwable) {
////          LOG.error("err", throwable);
////        } finally {
//        latch.countDown();
//        return true;
////        }
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    CommandCallback<JimCommand> callback2 = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        latch.countDown();
//        KvPair kvPair3 = kvCodec.encodeRecordRow(t, t.getColumns(), rows[2]);
//        RequestContext context3 = new RequestContext(t, routePolicy, kvPair3.getKey(),
//                Util.buildRawPut(kvPair3.getKey(), kvPair3.getValue()),
//                Api.RangeRequest.ReqCase.KV_PUT, timeout);
//        storeEngine.getDistSender().getShardSender().sendCommand(context3, callback3);
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    CommandCallback<JimCommand> callback = new CommandCallback<JimCommand>() {
//      @Override
//      protected boolean onSuccess0(JimCommand request, JimCommand response) {
//        latch.countDown();
//        KvPair kvPair2 = kvCodec.encodeRecordRow(t, t.getColumns(), rows[1]);
//        RequestContext context2 = new RequestContext(t, routePolicy, kvPair2.getKey(),
//                Util.buildRawPut(kvPair2.getKey(), kvPair2.getValue()), Api.RangeRequest.ReqCase.KV_PUT, timeout);
//        storeEngine.getDistSender().getShardSender().sendCommand(context2, callback2);
//        return true;
//      }
//
//      @Override
//      protected boolean onFailed0(JimCommand request, Throwable cause) {
//        LOG.error("response err:{} ", cause);
//        latch.countDown();
//        return true;
//      }
//    };
//
//    try {
//      KvPair kvPair = this.kvCodec.encodeRecordRow(t, t.getColumns(), rows[0]);
//      RequestContext context = new RequestContext(t, this.routePolicy, kvPair.getKey(),
//              Util.buildRawPut(kvPair.getKey(), kvPair.getValue()),
//              Api.RangeRequest.ReqCase.KV_PUT, timeout);
//      storeEngine.getDistSender().getShardSender().sendCommand(context, callback);
//    } catch (Throwable e) {
//      LOG.error("response err:{} ", e);
//    }
//
//    try {
//      latch.await();
//    } catch (InterruptedException e) {
//      LOG.error("wait err ", e);
//    }
//  }
//
//  @TearDown
//  public void after() {
//  }
//
//  public static void main(String[] args) throws RunnerException {
//    int threadNum = 100;
//    if (args.length > 0) {
//      threadNum = Integer.parseInt(args[0]);
//    }
//    System.out.println(threadNum);
//    Options opt = new OptionsBuilder().include(RpcBenchMark.class.getSimpleName())
//            .forks(1)
//            .threads(threadNum)
//            .warmupIterations(5)
//            .measurementIterations(10000000)
//            .build();
//    new Runner(opt).run();
//  }
//}
