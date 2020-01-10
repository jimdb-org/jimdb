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
package io.jimdb.test.engine;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.pb.Txn;

import com.google.protobuf.NettyByteString;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * @version V1.0
 */
public class FluxTest {
  public static void main(String[] args) {

    List<List<Txn.TxnIntent>> intentGroup = new ArrayList<>();

    List<Txn.TxnIntent> intentList = new ArrayList<>();
    Txn.TxnIntent intent = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("a".getBytes()))
            .setValue(NettyByteString.wrap("a".getBytes())).build();
    Txn.TxnIntent intent2 = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("b".getBytes()))
            .setValue(NettyByteString.wrap("b".getBytes())).build();
    intentList.add(intent);
    intentList.add(intent2);

    List<Txn.TxnIntent> intentList2 = new ArrayList<>();
    Txn.TxnIntent intent3 = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("c".getBytes()))
            .setValue(NettyByteString.wrap("c".getBytes())).build();
    Txn.TxnIntent intent4 = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("d".getBytes()))
            .setValue(NettyByteString.wrap("d".getBytes())).build();
    intentList2.add(intent3);
    intentList2.add(intent4);

    List<Txn.TxnIntent> intentList3 = new ArrayList<>();
    Txn.TxnIntent intent5 = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("e".getBytes()))
            .setValue(NettyByteString.wrap("e".getBytes())).build();
    Txn.TxnIntent intent6 = Txn.TxnIntent.newBuilder().setKey(NettyByteString.wrap("f".getBytes()))
            .setValue(NettyByteString.wrap("f".getBytes())).build();
    intentList3.add(intent5);
    intentList3.add(intent6);

    intentGroup.add(intentList);
    intentGroup.add(intentList2);
    intentGroup.add(intentList3);

    Flux<Object> flux = null;

    for (int i = 0; i < intentGroup.size(); i++) {
      String error = String.format("error %s", i);
      final int j = i;
      List<Txn.TxnIntent> intents = intentGroup.get(j);

      Flux<Object> child = Flux.create(sink -> sink.onRequest(l -> {
        System.out.println("start: " + j);
        sink.next(intents);
      }));
//      if (i == 0) {
      child = child.flatMap(list -> {
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {
          System.out.print(ex);
        }
        return Flux.just(list);
      });
//      }
      child = child.flatMap(list -> Flux.error(new Exception(error)));

//      if (i == 1) {
      child = child.onErrorResume(throwable -> {
        System.out.println("child flux : on error resume" + throwable);
        return Flux.just(intents);
      });
//      }
      if (flux == null) {
        flux = child;
      } else {
        flux = flux.publishOn(Schedulers.parallel()).zipWith(child, (f1, f2) -> f1);
      }
    }

    flux.subscribe(ok -> System.out.println("ok:" + ok), throwable -> System.out.println(throwable));
//    //java.lang.Exception: error 0

//    flux.onErrorResume(throwable -> {
//      System.out.println("on error resume" + throwable);
//      return Flux.error(throwable);
//    }).subscribe(ok -> System.out.println("subscribe ok: " + ok), throwable -> System.out.println("subscribe err:" + throwable));
//    on error resumejava.lang.Exception: error 0
//    subscribe err:java.lang.Exception: error 0

  }
}
