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
package io.jimdb.engine.table.alloc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.config.JimConfig;
import io.jimdb.meta.RouterManager;
import io.jimdb.meta.client.MasterClient;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * AutoIncrIDAllocator Tester.
 *
 * @version 1.0
 */
public class AutoIncrIDAllocatorTest {

  private List<Tuple2<Integer, Integer>> scope = new ArrayList<>();
  private int step = 5000;

  //  private MetaHttpManager metaManager;
  private ConcurrentLinkedQueue<UnsignedLongValue> rowIdQueue = new ConcurrentLinkedQueue<>();
  private static final UnsignedLongValue INTERVAL = UnsignedLongValue.getInstance(new BigInteger("1"));

  private ThreadPoolExecutor executor;
  private int num = 1000000;
  private RouterManager routerManager;

  @Before
  public void before() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("jim.meta.address", "http://11.3.83.36:443");
    properties.setProperty("jim.meta.cluster", "2");
    properties.setProperty("jim.meta.retry", "1");
    JimConfig config = new JimConfig(properties);

    scope.add(Tuples.of(0, 100000));
    scope.add(Tuples.of(230000, 330000));
    scope.add(Tuples.of(430000, 520000));
    this.executor = new ThreadPoolExecutor(20,
            20, 1, TimeUnit.MINUTES,
            new LinkedBlockingDeque<>(num));

    MasterClient msHttpClient = new MasterClient("http://11.3.83.36:443", 2);

    this.routerManager = new RouterManager(msHttpClient, config);
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: alloc()
   */
  @Test
  public void testAllocByLocal() throws Exception {
    ConcurrentHashMap<Long, Long> map = new ConcurrentHashMap();
    CountDownLatch latch = new CountDownLatch(num);
    for (int i = 0; i < num; i++) {
      executor.execute(() -> {
        try {
          UnsignedLongValue value = (UnsignedLongValue) alloc();
          if (map.get(value.getValue().longValue()) != null) {
            System.out.println("duplate:" + value.getString());
          } else {
            Long v = value.getValue().longValue();
            map.put(v, v);
          }
        } catch (RuntimeException e) {
          e.printStackTrace();
          System.out.println("executor err " + e.getMessage());
          return;
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.shutdown();
  }

//  private void syncRowIDScope() {
//    List<Mspb.IdPair> idPairList = mockRemoteAutoInc();
//    convertIdPairs(idPairList);
//    System.out.println("queue size" + rowIdQueue.size());
//  }
//
//  private List<Mspb.IdPair> mockRemoteAutoInc() {
//    List<Mspb.IdPair> idPairs = new ArrayList<>();
//    for (int i = 0; i < 3; i++) {
//      int start = scope.get(i).getT1();
//      int end = start + step;
//      int totalEnd = scope.get(i).getT2();
//      System.out.println("start:" + start + ";end:" + end);
//      idPairs.add(Mspb.IdPair.newBuilder().setIdStart(ByteString.copyFrom(ByteUtil.longToBytes(start)))
//              .setIdEnd(ByteString.copyFrom(ByteUtil.longToBytes(end))).build());
//      scope.set(i, Tuples.of(end, totalEnd));
//    }
//    return idPairs;
//  }
//
//  private void convertIdPairs(List<Mspb.IdPair> idPairs) {
//    List<List<UnsignedLongValue>> tempValues = new ArrayList<>();
//    int max = 0;
//    for (Mspb.IdPair idPair : idPairs) {
//      List<UnsignedLongValue> rangeValues = new ArrayList<>();
//      UnsignedLongValue startKey = ValueConvertor.convertToUnsignedLong(null,
//              BinaryValue.getInstance(NettyByteString.asByteArray(idPair.getIdStart())), null);
//      UnsignedLongValue endKey = ValueConvertor.convertToUnsignedLong(null,
//              BinaryValue.getInstance(NettyByteString.asByteArray(idPair.getIdEnd())), null);
//      while (startKey.compareToSafe(endKey) < 0) {
//        rangeValues.add(startKey);
//        startKey = (UnsignedLongValue) startKey.plus(null, INTERVAL);
//      }
//      if (!rangeValues.isEmpty()) {
//        tempValues.add(rangeValues);
//        if (max < rangeValues.size()) {
//          max = rangeValues.size();
//        }
//      }
//    }
//    for (int j = 0; j < max; j++) {
//      for (List<UnsignedLongValue> temp : tempValues) {
//        if (j >= temp.size()) {
//          break;
//        }
//        this.rowIdQueue.offer(temp.get(j));
//      }
//    }
//  }

  private Value alloc() {
    UnsignedLongValue value;
    if ((value = rowIdQueue.poll()) == null) {
      synchronized (this) {
        if ((value = rowIdQueue.poll()) == null) {
//          this.syncRowIDScope();
          value = rowIdQueue.poll();
        }
      }
    }
    return value;
  }

  @Test
  public void testAllocByRemote() throws Exception {
    for (int i = 0; i < num; i++) {
      try {
////        List<Mspb.IdPair> idPairList = this.metaManager.getAutoIncIds(3, 29, step, 1);
////        if (idPairList == null || idPairList.isEmpty()) {
////          System.out.println("sync auto increment from remote is null");
////        } else {
////          StringBuilder builder = new StringBuilder("remote return:");
////          for (Mspb.IdPair idPair : idPairList) {
////            builder.append("start:").append(ByteUtil.bytesToULong(idPair.getIdStart().toByteArray()))
////                    .append(";end:").append(ByteUtil.bytesToULong(idPair.getIdEnd().toByteArray()));
////
//////              UnsignedLongValue startKey = ValueConvertor.convertToUnsignedLong(null,
//////                      BinaryValue.getInstance(NettyByteString.asByteArray(idPair.getIdStart())), null);
//////              UnsignedLongValue endKey = ValueConvertor.convertToUnsignedLong(null,
//////                      BinaryValue.getInstance(NettyByteString.asByteArray(idPair.getIdEnd())), null);
////
//////              builder.append("start:").append(startKey.getString()).append(";end:").append(endKey.getString());
////          }
////          System.out.println("sync auto increment from remote is:" + builder);
//        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        System.out.println("executor err " + e.getMessage());
        return;
      }
    }
  }
}
