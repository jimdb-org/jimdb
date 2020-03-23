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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Defined interface for request dispatcher
 */
@SuppressFBWarnings()
public interface Dispatcher {

  void enqueue(Runnable runnable);

  void start();

  void close();

//  /**
//   * Send and process put request
//   * @param storeCtx store context
//   * @param kvPair key-value pair that will be put into the storage layer
//   * @return indication of success or failure
//   */
//  @Deprecated
//  Flux<Boolean> rawPut(StoreCtx storeCtx, KvPair kvPair);
//
//  /**
//   * Send and process the key get request
//   * @param storeCtx store context
//   * @param key key of the query
//   * @return value of the corresponding key
//   */
//  @Deprecated
//  Flux<byte[]> rawGet(StoreCtx storeCtx, byte[] key);
//
//  /**
//   * Send and process the delete request
//   * @param storeCtx store context
//   * @param key key to be deleted
//   * @return indication of success or failure
//   */
//  @Deprecated
//  Flux<Boolean> rawDel(StoreCtx storeCtx, byte[] key);
//
//  /**
//   * Send and process the prepare request of the transaction
//   * @param context request context
//   * @return flux of the response for the Prepare request
//   */
//  Flux<Txn.PrepareResponse> txnPrepare(RequestContext context);


//  /**
//   * Send and process the decide request of the transaction (commit or rollback)
//   * @param context request context
//   * @return flux of the response for the Decide request
//   */
//  Flux<Txn.DecideResponse> txnDecide(RequestContext context);
//
//  /**
//   * Send and process the cleanup request of the transaction
//   * @param context request context
//   * @return flux of the response for the Cleanup request
//   */
//  Flux<Txn.ClearupResponse> txnCleanup(RequestContext context);
//
//  /**
//   * Send and process a GetLockInfo request
//   * @param storeCtx store context
//   * @param request request builder
//   * @return requested LockInfo
//   */
//  Flux<Txn.GetLockInfoResponse> txnGetLockInfo(StoreCtx storeCtx, Txn.GetLockInfoRequest.Builder request);

//  /**
//   * Send and process a Scan request
//   * @param storeCtx store context
//   * @param reqBuilder request builder
//   * @return processed Scan result
//   */
//  Flux<List<Txn.KeyValue>> txnScan(StoreCtx storeCtx, Txn.ScanRequest.Builder reqBuilder);
//
//
//  /**
//   * Send and process a KeyGet request
//   * @param storeCtx store context
//   * @param reqBuilder request builder
//   * @return processed KeyGet result
//   */
//  Flux<List<Txn.Row>> txnKeyGet(StoreCtx storeCtx, Txn.SelectRequest.Builder reqBuilder);

//  /**
//   * Send and process a MultiKeySelect request
//   * @param storeCtx store context
//   * @param reqBuilder request builder
//   * @param keys keys the request are based on
//   * @return processed select result
//   */
//  Flux<List<Txn.Row>> txnMultiKeySelect(StoreCtx storeCtx, MessageOrBuilder reqBuilder, List<ByteString> keys);
//
//  /**
//   * Send and process a RangeSelect request
//   * @param storeCtx store context
//   * @param reqBuilder request builder
//   * @param kvPair kv pair the request is based on TODO explain
//   * @return processed select result
//   */
//  Flux<List<Txn.Row>> txnRangeSelect(StoreCtx storeCtx, MessageOrBuilder reqBuilder, KvPair kvPair);


//  /**
//   * Send and process a SingleRangeSelect request
//   * @param storeCtx store context
//   * @param reqBuilder request builder
//   * @param kvPair kv pair the request is based on TODO explain
//   * @return processed select result
//   */
//  Flux<List<Txn.Row>> txnSingleRangeSelect(StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder, KvPair kvPair);
//
//  /**
//   * Send analyzeIndex request to the storage layer
//   *
//   * @param storeCtx   store context
//   * @param reqBuilder request builder
//   * @return flux of the response from the storage layer.
//   */
//  Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(StoreCtx storeCtx, Statspb.IndexStatsRequest.Builder reqBuilder);
//
//  /**
//   * Send analyzeColumns request to the storage layer
//   *
//   * @param storeCtx   store context
//   * @param reqBuilder request builder
//   * @return flux of the response from the storage layer
//   */
//  Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(StoreCtx storeCtx, Statspb.ColumnsStatsRequest.Builder reqBuilder);
}
