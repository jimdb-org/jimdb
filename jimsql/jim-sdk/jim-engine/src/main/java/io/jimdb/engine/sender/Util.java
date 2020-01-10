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
package io.jimdb.engine.sender;

import java.util.List;

import io.jimdb.engine.txn.TxnConfig;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * command Util
 * rpc type
 *
 * @version V1.0
 */
public class Util {
  private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

  public static Message.Builder buildRawPut(ByteString key, ByteString value) {
    return Kv.KvPutRequest.newBuilder().setKey(key).setValue(value);
  }

  public static Message.Builder buildRawGet(ByteString key) {
    return Kv.KvGetRequest.newBuilder().setKey(key);
  }

  public static Message.Builder buildRawDelete(ByteString key) {
    return Kv.KvDeleteRequest.newBuilder().setKey(key);
  }

  public static Txn.PrepareRequest.Builder buildPrepare4Primary(TxnConfig config) {
    Txn.PrepareRequest.Builder bodyBuilder = Txn.PrepareRequest.newBuilder()
            .setTxnId(config.getTxnId())
            .setLocal(config.isLocal())
            .addIntents(config.getPriIntent())
            .setPrimaryKey(config.getPriIntent().getKey())
            .setLockTtl(config.getLockTTl())
            .setStrictCheck(true);
    List<Txn.TxnIntent> secIntents = config.getSecIntents();
    if (secIntents != null && secIntents.size() > 0) {
      for (Txn.TxnIntent intent : secIntents) {
        bodyBuilder.addSecondaryKeys(intent.getKey());
      }
    }
    return bodyBuilder;
  }

  public static Txn.PrepareRequest.Builder buildPrepare4Secondary(TxnConfig config, List<Txn.TxnIntent> intents) {
    return Txn.PrepareRequest.newBuilder()
            .setTxnId(config.getTxnId())
            .setPrimaryKey(config.getPriIntent().getKey())
            .addAllIntents(intents)
            .setLockTtl(config.getLockTTl())
            .setStrictCheck(true);
  }

  public static Txn.DecideRequest.Builder buildTxnDecide4Primary(TxnConfig config, Txn.TxnStatus status) {
    return Txn.DecideRequest.newBuilder()
            .setTxnId(config.getTxnId())
            .setStatus(status)
            .addKeys(config.getPriIntent().getKey())
            .setIsPrimary(true);
  }

//  public static Txn.DecideRequest.Builder buildTxnDecide4Secondary(TxnConfig config) {
//    Txn.DecideRequest.Builder bodyBuilder = Txnpb.DecideRequest.newBuilder()
//            .setTxnId(config.getTxnId())
//            .setIsPrimary();
//    return bodyBuilder;
//  }
}
