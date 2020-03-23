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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.pb.Api;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Statspb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * TODO
 */
@SuppressFBWarnings()
public class AnalyzeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeHandler.class);

  private static final RequestHandler.RequestFunc<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>, Statspb.IndexStatsRequest.Builder>
      SEND_ANALYZE_INDEX_REQ_FUNC = AnalyzeHandler::sendAnalyzeIndexReq;

  private static final RequestHandler.RequestFunc<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>, Statspb.ColumnsStatsRequest.Builder>
      SEND_ANALYZE_COLUMNS_REQ_FUNC = AnalyzeHandler::sendAnalyzeColumnsReq;


  /* Top level callee by the analyze executor */

  /**
   * Send analyzeIndex request to the storage layer
   *
   * @param storeCtx   store context
   * @param reqBuilder request builder
   * @return flux of the response from the storage layer.
   */
  public static Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>>
      analyzeIndex(ShardSender shardSender, StoreCtx storeCtx, Statspb.IndexStatsRequest.Builder reqBuilder) {

    final ByteString startKey = reqBuilder.getRange().getStartKey();
    final ByteString endKey = reqBuilder.getRange().getEndKey();

    // TODO make this as a function apply
    return RangeSelector.rangeSelect(shardSender, storeCtx, SEND_ANALYZE_INDEX_REQ_FUNC, reqBuilder, startKey, endKey);
  }

  // SEND_ANALYZE_INDEX_REQ_FUNC
  private static Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>>
      sendAnalyzeIndexReq(ShardSender shardSender, StoreCtx storeCtx, Statspb.IndexStatsRequest.Builder reqBuilder, ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.INDEX_STATS);

    return shardSender.sendReq(context).map(r -> {
      Statspb.IndexStatsResponse response = (Statspb.IndexStatsResponse) r;
      if (response.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "analyzeIndex", String.valueOf(response.getCode()));
      }

      // note that this message may appear multiple times in the log because it is called within a loop
      LOGGER.debug("received response for analyzing index with columns {}: {}",
          reqBuilder.getColumnsInfoList().stream().map(Exprpb.ColumnInfo::getId).collect(Collectors.toList()),
          response);

      return Collections.singletonList(Tuples.of(response.getHist(), response.getCms()));
    });
  }

  /**
   * Send analyzeColumns request to the storage layer
   *
   * @param storeCtx   store context
   * @param reqBuilder request builder
   * @return flux of the response from the storage layer
   */
  public static Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(ShardSender shardSender, StoreCtx storeCtx,
                                                                                                    Statspb.ColumnsStatsRequest.Builder reqBuilder) {
    final ByteString startKey = reqBuilder.getRange().getStartKey();
    final ByteString endKey = reqBuilder.getRange().getEndKey();

    // TODO make this as a function apply
    return RangeSelector.rangeSelect(shardSender, storeCtx, SEND_ANALYZE_COLUMNS_REQ_FUNC, reqBuilder, startKey, endKey);
  }

  // SEND_ANALYZE_COLUMNS_REQ_FUNC
  private static Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> sendAnalyzeColumnsReq(
      ShardSender shardSender, StoreCtx storeCtx, Statspb.ColumnsStatsRequest.Builder reqBuilder, ByteString key, RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.COLUMNS_STATS);

    return shardSender.sendReq(context).map(r -> {
      Statspb.ColumnsStatsResponse response = (Statspb.ColumnsStatsResponse) r;
      if (response.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "analyzeColumns", String.valueOf
                                                                                                                 (response.getCode()));
      }

      LOGGER.debug("received response for analyzing columns {}: {}",
          reqBuilder.getColumnsInfoList().stream().map(Exprpb.ColumnInfo::getId).collect(Collectors.toList()),
          response);

      return Collections.singletonList(Tuples.of(response.getPkHist(), response.getCollectorsList()));
    });
  }
}
