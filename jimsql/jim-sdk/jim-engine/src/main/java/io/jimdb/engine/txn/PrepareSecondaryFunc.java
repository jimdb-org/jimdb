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
package io.jimdb.engine.txn;

import java.util.List;

import io.jimdb.engine.StoreCtx;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.pb.Txn;

import com.google.protobuf.ByteString;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */

@FunctionalInterface
interface PreparePrimaryFunc {
  Flux<ExecResult> apply(StoreCtx context, TxnConfig config);
}

/**
 * @version V1.0
 */

@FunctionalInterface
interface DecidePrimaryFunc {
  Flux<ExecResult> apply(StoreCtx context, TxnConfig config, Txn.TxnStatus status);
}

/**
 * @param <T>
 * @version V1.0
 */

@FunctionalInterface
interface PrepareSecondaryFunc<T> {
  Flux<ExecResult> apply(StoreCtx context, TxnConfig config, List<T> list);
}

/**
 * @param <T>
 * @version V1.0
 */

@FunctionalInterface
interface DecideSecondaryFunc<T> {
  Flux<Boolean> apply(StoreCtx context, String txnId, List<T> list, Txn.TxnStatus status);
}

/**
 * @version V1.0
 */

@FunctionalInterface
interface RecoverFunc {
  Flux<ExecResult> apply(StoreCtx context, TxnConfig config);
}

/**
 * @version V1.0
 */

@FunctionalInterface
interface CleanUpFunc {
  Flux<Txn.ClearupResponse> apply(StoreCtx context, TxnConfig config);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface SelectFunc {
  Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectRequest.Builder reqBuilder, ColumnExpr[] exprs);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface SelectFlowFunc {
  Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs, KvPair kvPair);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface SelectFlowRangeFunc {
  Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs,
                              KvPair kvPair);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface SelectFlowKeysFunc {
  Flux<ValueAccessor[]> apply(StoreCtx context, Txn.SelectFlowRequest.Builder reqBuilder, ColumnExpr[] exprs,
                              List<ByteString> keys);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface ScanPKFunc {
  Flux<List<ByteString>> apply(StoreCtx storeCtx, Txn.ScanRequest.Builder request, Index index);
}

/**
 * @version V1.0
 */
@FunctionalInterface
interface ScanVerFunc {
  Flux<Long> apply(StoreCtx storeCtx, ByteString key, Index index);
}

