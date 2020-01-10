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
package io.jimdb.engine.txn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Txn;
import io.jimdb.common.utils.generator.UUIDGenerator;
import io.jimdb.common.utils.lang.ByteUtil;

import com.google.protobuf.ByteString;

/**
 * @version V1.0
 */
public class TxnConfig {
  public static final int TXN_INTENT_MAX_LENGTH = 100;

  private String txnId;
  /**
   * default transaction lock timeout is 20000 ms
   */
  private long ttl = 20000;
  private List<Txn.TxnIntent> intents;

  private Table table;

  TxnConfig(long ttl) {
    this.txnId = generateTxnId();
    if (ttl > 0) {
      this.ttl = ttl;
    }
  }

  public String getTxnId() {
    return this.txnId == null ? "" : this.txnId;
  }

  public long getLockTTl() {
    return this.ttl;
  }

  public Table getTable() {
    return this.table;
  }

  public List<Txn.TxnIntent> getIntents() {
    return this.intents;
  }

  public void addIntent(Txn.TxnIntent txnIntent, Table table) {
    if (txnIntent == null || table == null) {
      return;
    }
    if (getIntents() == null) {
      synchronized (this) {
        if (getIntents() == null) {
          this.intents = new ArrayList<>();
          this.table = table;
        }
      }
    }
    if (this.intents.size() > TXN_INTENT_MAX_LENGTH) {
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_TOO_MANY_CONCURRENT_TRXS);
    }
    this.intents.add(txnIntent);
  }

  public boolean emptyIntents() {
    if (getIntentSize() == 0) {
      return true;
    }
    return false;
  }

  private int getIntentSize() {
    if (this.intents == null || this.intents.isEmpty()) {
      return 0;
    }
    return this.intents.size();
  }

  private String generateTxnId() {
    return UUIDGenerator.next();
  }

  public void sortIntents() {
    Collections.sort(this.intents, (o1, o2) -> ByteUtil.compare(o1.getKey(), o2.getKey()));
    synchronized (this) {
      Txn.TxnIntent priIntent = this.intents.get(0);
      this.intents.set(0, Txn.TxnIntent.newBuilder(priIntent).setIsPrimary(true).build());
    }
  }

  public Txn.TxnIntent getPriIntent() {
    if (getIntentSize() == 0) {
      return null;
    }
    return this.intents.get(0);
  }

  public List<Txn.TxnIntent> getSecIntents() {
    if (getIntentSize() <= 1) {
      return null;
    }
    return this.intents.subList(1, this.intents.size());
  }

  public List<ByteString> getSecKeys() {
    if (getIntentSize() <= 1) {
      return null;
    }
    List<ByteString> keys = new ArrayList<>();
    this.intents.subList(1, this.intents.size()).stream()
            .forEach(intent -> keys.add(intent.getKey()));
    return keys;
  }

  public boolean isLocal() {
    if (getIntentSize() > 1) {
      return false;
    }
    return true;
  }
}
