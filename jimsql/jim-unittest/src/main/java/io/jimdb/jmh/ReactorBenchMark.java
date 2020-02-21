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
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//
//import io.jimdb.config.JimConfig;
//import io.jimdb.engine.JimStoreEngine;
//import io.jimdb.model.Table;
//import io.jimdb.pb.Kv;
//import io.jimdb.plugin.MetaEngine;
//import io.jimdb.plugin.store.Engine;
//import io.jimdb.Bootstraps;
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
//import reactor.core.publisher.Flux;
//
///**
// * @version V1.0
// */
////@BenchmarkMode(Mode.AverageTime) // 测试方法平均执行时间
//@BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
//@OutputTimeUnit(TimeUnit.SECONDS)// 输出结果的时间粒度为秒
//@State(Scope.Benchmark) // 每个测试线程一个实例
//public class ReactorBenchMark {
//  private static final Logger LOG = LoggerFactory.getLogger(EngineBenchMark.class);
//  private static final AtomicLong ID = new AtomicLong(0L);
//
//  JimStoreEngine engine;
//  MetaEngine meta;
//  String catalog;
//  String tableName;
//  Table t;
//
//  public ReactorBenchMark() {
//  }
//
//  @Setup(Level.Trial)
//  public void init() {
//    try {
//      JimConfig config = Bootstraps.init("jim.properties");
//      this.catalog = config.getString("catalog", "maggie");
//      this.tableName = config.getString("table.name", "maggie2");
//      this.meta = config.getMetaEngine();
//      Engine e = config.getStoreEngine();
//      if (!(e instanceof JimStoreEngine)) {
//        throw new IllegalArgumentException("engine error");
//      }
//      this.engine = (JimStoreEngine) e;
//      this.t = this.meta.getMetaData().getTable(this.catalog, this.tableName);
//    } catch (Throwable var2) {
//      var2.printStackTrace();
//    }
//  }
//
//  @Benchmark
//  public void testFluxMethod() {
//    Object[][] rows = new Object[3][3];
//    for (int i = 0; i < 3; i++) {
//      long rowId = ID.incrementAndGet();
//      Object[] row = new Object[]{ rowId, String.format("%d", rowId), null };
//      row[2] = row[1];
//      rows[i] = row;
//    }
//
//    CountDownLatch latch = new CountDownLatch(1);
//    try {
//      Flux<Kv.KvPutResponse> flux = this.engine.rawPut(t, t.getColumns(), rows[0])
//              .flatMap(r -> this.engine.rawPut(t, t.getColumns(), rows[1]))
//              .flatMap(r -> this.engine.rawPut(t, t.getColumns(), rows[2]));
//      flux.subscribe(m -> latch.countDown(), e -> {
//        LOG.error("response err:{} ", e.getMessage());
//        latch.countDown();
//      });
//    } catch (Throwable e2) {
//      LOG.error("throw  err ", e2);
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
//    Options opt = new OptionsBuilder().include(ReactorBenchMark.class.getSimpleName())
//            .forks(1)
//            .threads(100)
//            .warmupIterations(5)
//            .measurementIterations(10000000)
//            .build();
//    new Runner(opt).run();
//  }
//}
