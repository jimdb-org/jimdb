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
package io.jimdb.meta.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.common.exception.ClosedClientException;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class EtcdClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(EtcdClient.class);

  private static final long REQ_TIMEOUT = 10000L;

  private static EtcdClient clientInstance = null;

  private final long leaseId;
  private final Client etcdClient;
  private final Watch watchClient;
  private final Lease leaseClient;
  private final KV kvClient;
  private final List<Watch.Watcher> watchers;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public static synchronized EtcdClient getInstance(String endpoints, long ttl, TimeUnit unit) {
    if (clientInstance == null || clientInstance.isClosed()) {
      clientInstance = new EtcdClient(endpoints, ttl, unit);
    }

    return clientInstance;
  }

  private EtcdClient(String endpoints, long ttl, TimeUnit unit) {
    this.etcdClient = Client.builder().endpoints(endpoints).build();
    this.watchClient = this.etcdClient.getWatchClient();
    this.leaseClient = this.etcdClient.getLeaseClient();
    this.kvClient = this.etcdClient.getKVClient();
    this.watchers = new ArrayList<>();
    this.leaseId = initLease(unit.toSeconds(ttl));
  }

  private long initLease(long ttl) {
    ttl = ttl < 20 ? 20 : ttl;
    try {
      long leaseId = leaseClient.grant(ttl).get(REQ_TIMEOUT, TimeUnit.MILLISECONDS).getID();
      leaseClient.keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
        @Override
        public void onNext(LeaseKeepAliveResponse value) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Lease KeepAlive Response value:{}", value.getTTL());
          }
        }

        @Override
        public void onError(Throwable t) {
          if (!(t instanceof ClosedClientException)) {
            LOGGER.error("Lease KeepAlive error occurred", t);
          }
        }

        @Override
        public void onCompleted() {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Lease KeepAlive completed");
          }
        }
      });
      return leaseId;
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Lease Init");
    }
  }

  private ByteSequence getByteSequence(String key) {
    return ByteSequence.from(key, StandardCharsets.UTF_8);
  }

  public ByteSequence comparePutAndGet(ByteSequence key, ByteSequence expect, ByteSequence update, boolean isLease) {
    try {
      PutOption putOption = PutOption.DEFAULT;
      if (isLease) {
        putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
      }

      Txn txn = kvClient.txn();
      txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(expect)));
      txn.Then(Op.PutOp.put(key, update, putOption));
      txn.Else(Op.get(key, GetOption.DEFAULT));
      TxnResponse responses = txn.commit().get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!responses.getPutResponses().isEmpty()) {
        return update;
      }
      return responses.getGetResponses().get(0).getKvs().get(0).getValue();
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Compare And Put key(" + key.toString(StandardCharsets.UTF_8) + ")");
    }
  }

  private boolean compareAndPut(ByteSequence key, ByteSequence expect, ByteSequence update, boolean isLease) {
    try {
      PutOption putOption = PutOption.DEFAULT;
      if (isLease) {
        putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
      }

      Txn txn = kvClient.txn();
      if (expect == null) {
        txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0)));
      } else {
        txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(expect)));
      }
      txn.Then(Op.PutOp.put(key, update, putOption));
      List<PutResponse> responses = txn.commit().get(REQ_TIMEOUT, TimeUnit.MILLISECONDS).getPutResponses();
      return !responses.isEmpty();
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Compare And Put key(" + key.toString(StandardCharsets.UTF_8) + ")");
    }
  }

  private boolean compareAndRemove(ByteSequence key, ByteSequence expect) {
    try {
      Txn txn = kvClient.txn();
      txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(expect)));
      txn.Then(Op.PutOp.delete(key, DeleteOption.DEFAULT));
      List<DeleteResponse> deleteResponses = txn.commit().get(REQ_TIMEOUT, TimeUnit.MILLISECONDS).getDeleteResponses();
      return !deleteResponses.isEmpty();
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Compare And Remove key(" + key.toString(StandardCharsets.UTF_8) + ")");
    }
  }

  public ByteSequence get(String key) {
    try {
      CompletableFuture<GetResponse> future = kvClient.get(getByteSequence(key));
      GetResponse getResponse = future.get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!getResponse.isMore() && !getResponse.getKvs().isEmpty()) {
        return getResponse.getKvs().get(0).getValue();
      }
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Get key(" + key + ")");
    }
    return null;
  }

  public void put(String key, ByteString value, boolean isLease) {
    try {
      PutOption putOption = PutOption.DEFAULT;
      if (isLease) {
        putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
      }
      kvClient.put(getByteSequence(key), ByteSequence.from(value), putOption).get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Put Key(" + key + ")");
    }
  }

  public boolean comparePut(String key, ByteString expect, ByteString value) {
    return this.compareAndPut(getByteSequence(key), expect == null ? null : ByteSequence.from(expect), ByteSequence.from(value), false);
  }

  public String putIfAbsent(String key, String value) {
    return this.putIfAbsent(key, value, false);
  }

  private String putIfAbsent(String key, String value, boolean isLease) {
    ByteSequence result = putIfAbsent(getByteSequence(key), getByteSequence(value), isLease);
    return result.toString(StandardCharsets.UTF_8);
  }

  public ByteSequence putIfAbsent(ByteSequence key, ByteSequence value, boolean isLease) {
    try {
      PutOption putOption = PutOption.DEFAULT;
      if (isLease) {
        putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
      }

      Txn txn = kvClient.txn();
      txn.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0)));
      txn.Then(Op.PutOp.put(key, value, putOption));
      txn.Else(Op.get(key, GetOption.DEFAULT));
      TxnResponse responses = txn.commit().get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);

      if (!responses.getPutResponses().isEmpty()) {
        return value;
      }
      return responses.getGetResponses().get(0).getKvs().get(0).getValue();
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Put IfAbsent key(" + key.toString(StandardCharsets.UTF_8) + ")");
    }
  }

  public boolean putIfAbsent(List<Tuple2<String, ByteString>> values) {
    try {
      List<Op.PutOp> ops = new ArrayList<>(values.size());
      for (Tuple2<String, ByteString> value : values) {
        ops.add(Op.PutOp.put(getByteSequence(value.getT1()), ByteSequence.from(value.getT2()), PutOption.DEFAULT));
      }

      Txn txn = kvClient.txn();
      txn.If(new Cmp(getByteSequence(values.get(0).getT1()), Cmp.Op.EQUAL, CmpTarget.createRevision(0)));
      txn.Then(ops.toArray(new Op.PutOp[0]));
      TxnResponse responses = txn.commit().get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);
      return !responses.getPutResponses().isEmpty();
    } catch (Throwable ex) {
      List<String> keys = new ArrayList<>(values.size());
      for (Tuple2<String, ByteString> value : values) {
        keys.add(value.getT1());
      }
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, String.format("Etcd Put IfAbsent key %s", keys.toString()));
    }
  }

  public void delete(String key) {
    try {
      String prefix = key;
      if (!prefix.endsWith("/")) {
        prefix = prefix + "/";
      }

      DeleteOption.Builder builder = DeleteOption.newBuilder();
      builder.withPrefix(getByteSequence(prefix));
      kvClient.delete(getByteSequence(key), builder.build()).get(REQ_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Delete Key(" + key + ")");
    }
  }

  public long incrAndGet(String key, long incr) {
    while (true) {
      ByteSequence byteKey = getByteSequence(key);
      String value = this.putIfAbsent(key, "0", false);
      long nextId = Long.parseLong(value) + incr;

      boolean result = this.compareAndPut(byteKey, getByteSequence(value), getByteSequence(String.valueOf(nextId)), false);
      if (result) {
        return nextId;
      }
    }
  }

  public void watch(String key, boolean noUpdate, LongConsumer consumer) {
    try {
      Watch.Listener listener = Watch.listener(response -> {
        for (WatchEvent event : response.getEvents()) {
          if (noUpdate && event.getKeyValue().getCreateRevision() != event.getKeyValue().getModRevision()) {
            continue;
          }

          consumer.accept(0);
        }
      }, throwable -> LOGGER.error("Watch Key(" + key + ") error occurred", throwable));

      ByteSequence byteKey = getByteSequence(key);
      WatchOption option = WatchOption.newBuilder()
              .withPrefix(byteKey)
              .withNoDelete(true)
              .build();
      Watch.Watcher watch = watchClient.watch(byteKey, option, listener);
      watchers.add(watch);
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd Watch Key(" + key + ")");
    }
  }

  public boolean tryLock(String key, String value) {
    String existValue = this.putIfAbsent(key, value, true);
    return value.equals(existValue);
  }

  public boolean unLock(String key, String value) {
    return this.compareAndRemove(getByteSequence(key), getByteSequence(value));
  }

  public Map<String, ByteSequence> listValues(String key, boolean allChild) {
    Map<String, ByteSequence> result = new HashMap<>();
    try {
      GetOption getOption = GetOption.newBuilder()
              .withPrefix(getByteSequence(key))
              .build();

      CompletableFuture<GetResponse> future = kvClient.get(getByteSequence(key), getOption);
      List<KeyValue> kvs = future.get(REQ_TIMEOUT, TimeUnit.MILLISECONDS).getKvs();
      for (KeyValue kv : kvs) {
        String k = kv.getKey().toString(StandardCharsets.UTF_8);
        if (allChild) {
          result.put(k, kv.getValue());
          continue;
        }

        String tmp = StringUtils.removeStart(k, key);
        if (tmp.indexOf('/') == -1) {
          result.put(k, kv.getValue());
        }
      }
    } catch (Throwable ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_STORE_ERROR, ex, "Etcd List Key(" + key + ")");
    }

    return result;
  }

  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    for (Watch.Watcher watcher : watchers) {
      watcher.close();
    }
    if (etcdClient != null) {
      this.etcdClient.close();
    }
  }

  private boolean isClosed() {
    return closed.get();
  }
}
