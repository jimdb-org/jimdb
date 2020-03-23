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

import static java.util.Collections.EMPTY_LIST;
import static io.jimdb.engine.KeyGetter.KEY_GET_FUNC;
import static io.jimdb.engine.MultiKeySelector.MULTI_KEY_SELECT_FUNC;
import static io.jimdb.engine.RangeSelector.RANGE_SELECT_FUNC;
import static io.jimdb.engine.ScanHandler.SCAN_PK_FUNC;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.BaseException;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.Session;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.codec.KvPair;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.values.Value;
import io.jimdb.engine.client.RequestContext;
import io.jimdb.meta.Router;
import io.jimdb.pb.Api;
import io.jimdb.pb.Exprpb;
import io.jimdb.pb.Kv;
import io.jimdb.pb.Processorpb;
import io.jimdb.pb.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;

/**
 * TODO
 */
@SuppressFBWarnings()
public class SelectHandler extends RequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectHandler.class);


  public static final RequestFunc<List<Txn.Row>, Txn.SelectFlowRequest.Builder> SEND_SELECT_REQ_FUNC = SelectHandler::sendSelectReq;

  /**
   * @param session
   * @param indexes
   * @param values
   * @param resultColumns
   * @return
   */
  public static Flux<ExecResult> get(ShardSender shardSender, Router router, Session session, List<Index> indexes, List<Value[]> values, ColumnExpr[] resultColumns) {
    if (indexes == null || indexes.isEmpty()) {
      return Flux.create(sink -> sink.next(new QueryExecResult(resultColumns, ROW_VALUE_ACCESSORS_EMPTY)));
    }

    Instant timeout = session.getStmtContext().getTimeout();


    Flux<ValueAccessor[]> result = null;
    for (int i = 0; i < indexes.size(); i++) {
      Flux<ValueAccessor[]> subResult = null;
      Index index = indexes.get(i);
      Value[] indexVals = values.get(i);
      Table table = index.getTable();
      StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
      Txn.SelectRequest.Builder reqBuilder = Txn.SelectRequest.newBuilder()
                                                 .addAllFieldList(getSelectFields(resultColumns));
      if (index.isPrimary()) {
        reqBuilder.setKey(Codec.encodeKey(table.getId(), index.getColumns(), indexVals));
        subResult = KEY_GET_FUNC.apply(shardSender, storeCtx, reqBuilder, resultColumns)
                        .onErrorResume(KeyGetter.getErrHandler(shardSender, storeCtx, KEY_GET_FUNC, reqBuilder, resultColumns));
      } else if (index.isUnique()) {
        //indexValue cannot exist null
        for (int j = 0; j < indexVals.length; j++) {
          Value value = indexVals[j];
          if (value == null || value.isNull()) {
            subResult = Flux.just(ROW_VALUE_ACCESSORS_ARRAY_EMPTY);
            break;
          }
        }
        if (subResult != null) {
          continue;
        }
        ByteString idxKey = Codec.encodeIndexKey(index, values.get(i), ByteString.EMPTY, false);
        Txn.ScanRequest.Builder request = Txn.ScanRequest.newBuilder()
                                              .setStartKey(idxKey)
                                              .setEndKey(Codec.nextKey(idxKey))
                                              .setOnlyOne(true);
        Flux<List<ByteString>> searchPkFlux = SCAN_PK_FUNC.apply(shardSender, storeCtx, request, index)
                                                  .onErrorResume(ScanHandler.getErrHandler(shardSender, storeCtx, SCAN_PK_FUNC, request, index));
        subResult = searchPkFlux.flatMap(pks -> {
          if (pks == null || pks.isEmpty()) {
            return Flux.just(ROW_VALUE_ACCESSORS_ARRAY_EMPTY);
          }
          if (pks.size() > 1) {
            throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_DUP_UNKNOWN_IN_INDEX, index.getName());
          }
          reqBuilder.setKey(Codec.encodeTableKey(table.getId(), pks.get(0)));
          return KEY_GET_FUNC.apply(shardSender, storeCtx, reqBuilder, resultColumns)
                     .onErrorResume(KeyGetter.getErrHandler(shardSender, storeCtx, KEY_GET_FUNC, reqBuilder, resultColumns));
        });
      } else {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_NOT_SUPPORTED_YET, "NOT supported with non-unique condition index.");
      }
      if (result == null) {
        result = subResult;
      } else {
        result = result.zipWith(subResult, SelectHandler::getValueAccessors);
      }
    }
    return result.map(rows -> new QueryExecResult(resultColumns, rows));
  }

  private static ValueAccessor[] getValueAccessors(ValueAccessor[] f1, ValueAccessor[] f2) {
    if (f1 == null || f1.length == 0) {
      return f2;
    }
    if (f2 == null || f2.length == 0) {
      return f1;
    }
    ValueAccessor[] merge = new ValueAccessor[f1.length + f2.length];
    System.arraycopy(f1, 0, merge, 0, f1.length);
    System.arraycopy(f2, 0, merge, f1.length, f2.length);
    return merge;
  }

  private static List<Txn.SelectField> getSelectFields(ColumnExpr[] resultColumns) {
    List<Txn.SelectField> selectFields = new ArrayList<>(resultColumns.length);
    //select fields
    for (ColumnExpr column : resultColumns) {
      Txn.SelectField.Builder field = Txn.SelectField.newBuilder()
                                          .setColumn(convertColumnBuilder(column))
                                          .setTyp(Txn.SelectField.Type.Column);
      selectFields.add(field.build());
    }
    return selectFields;
  }

  private static Exprpb.ColumnInfo.Builder convertColumnBuilder(ColumnExpr expr) {
    Exprpb.ColumnInfo.Builder builder = Exprpb.ColumnInfo.newBuilder()
                                            .setId(expr.getId().intValue())
                                            .setTyp(expr.getResultType().getType())
                                            .setUnsigned(expr.getResultType().getUnsigned());

    if (expr.getReorgValue() != null) {
      builder.setReorgValue(expr.getReorgValue());
    }
    return builder;
  }

  /**
   * @param table
   * @param processors
   * @param resultColumns
   * @param outputOffsetList
   * @return
   * @throws BaseException
   */
  public static Flux<ExecResult> select(ShardSender shardSender, Router router, Session session, Table table, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                                        List<Integer> outputOffsetList) throws BaseException {
    if (processors == null || processors.isEmpty()) {
      return convertToExecResult(resultColumns, null);
    }

    StoreCtx storeCtx = StoreCtx.buildCtx(session, table, router);
    KvPair kvPair = Codec.encodeTableScope(table.getId());
    Flux<ValueAccessor[]> valuesFlux = fetchRangeSelectFlux(shardSender, processors, resultColumns, outputOffsetList, storeCtx,
        kvPair, true);
    return convertToExecResult(resultColumns, valuesFlux);
  }

  /**
   * @param index
   * @param processors
   * @param resultColumns
   * @param outputOffsetList
   * @param ranges
   * @return
   * @throws BaseException
   */
  public static Flux<ExecResult> select(ShardSender shardSender, Router router, Session session, Index index, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                                        List<Integer> outputOffsetList, List<ValueRange> ranges) throws BaseException {
    if (processors == null || processors.isEmpty()) {
      return convertToExecResult(resultColumns, null);
    }

    boolean isPrimary = index.isPrimary();
    boolean isOptimizeKey = isPrimary || index.isUnique();
    List<ByteString> keys = EMPTY_LIST;
    List<KvPair> rangePairs;
    List<KvPair> kvPairs = Codec.encodeKeyRanges(index, ranges, isOptimizeKey);
    if (isOptimizeKey && !kvPairs.isEmpty()) {   // when primary-key or unique could use both of key and range
      rangePairs = new ArrayList<>(kvPairs.size());
      keys = new ArrayList<>(kvPairs.size());
      for (KvPair kvPair : kvPairs) {
        if (kvPair.isKey()) {
          keys.add(kvPair.getKey());
        } else {
          rangePairs.add(kvPair);
        }
      }
    } else {
      rangePairs = kvPairs;
    }

    StoreCtx storeCtx = StoreCtx.buildCtx(session, index.getTable(), router);

    Flux<ValueAccessor[]> valuesFlux = fetchMultiKeySelectFlux(shardSender, processors, resultColumns, outputOffsetList, storeCtx, keys, isPrimary);

    for (KvPair kvPair : rangePairs) {
      Flux<ValueAccessor[]> valuesEachRangeFlux = fetchRangeSelectFlux(shardSender, processors, resultColumns, outputOffsetList,
          storeCtx, kvPair, isPrimary);
      if (valuesFlux == null) {
        valuesFlux = valuesEachRangeFlux;
      } else {
        valuesFlux = valuesFlux.zipWith(valuesEachRangeFlux, SelectHandler::getValueAccessors);
      }
    }
    return convertToExecResult(resultColumns, valuesFlux);
  }

  private static Flux<ValueAccessor[]> fetchRangeSelectFlux(ShardSender shardSender, List<Processorpb.Processor.Builder> processors,
                                                            ColumnExpr[] resultColumns, List<Integer> outputOffsetList,
                                                            StoreCtx storeCtx, KvPair kvPair, boolean isPrimary) {

    Txn.SelectFlowRequest.Builder builder = isPrimary ? buildTableReaderReqForRange(processors, outputOffsetList, kvPair)
                                                : buildIndexReaderReqForRange(processors, outputOffsetList, kvPair);

    return RANGE_SELECT_FUNC.apply(shardSender, storeCtx, builder, resultColumns, kvPair)
               .onErrorResume(RangeSelector.getErrHandler(shardSender, storeCtx, RANGE_SELECT_FUNC, builder, resultColumns, kvPair));
  }

  private static Txn.SelectFlowRequest.Builder buildIndexReaderReqForRange(List<Processorpb.Processor.Builder> processors,
                                                                           List<Integer> outputOffsetList, KvPair kvPair) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.INDEX_READ_TYPE) {
        Processorpb.IndexRead.Builder indexRead = Processorpb.IndexRead.newBuilder(processor.getIndexRead())
                                                      .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                                                      .setRange(Kv.KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setIndexRead(indexRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private static Txn.SelectFlowRequest.Builder buildTableReaderReqForRange(List<Processorpb.Processor.Builder> processors,
                                                                           List<Integer> outputOffsetList,
                                                                           KvPair kvPair) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.TABLE_READ_TYPE) {
        Processorpb.TableRead.Builder tableRead = Processorpb.TableRead.newBuilder(processor.getTableRead())
                                                      .setType(Processorpb.KeyType.KEYS_RANGE_TYPE)
                                                      .setRange(Kv.KeyRange.newBuilder().setStartKey(kvPair.getKey()).setEndKey(kvPair.getValue()));
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private static Flux<ValueAccessor[]> fetchMultiKeySelectFlux(ShardSender shardSender, List<Processorpb.Processor.Builder> processors, ColumnExpr[] resultColumns,
                                                               List<Integer> outputOffsetList, StoreCtx storeCtx,
                                                               List<ByteString> keys, boolean isPrimary) {
    if (keys.isEmpty()) {
      return null;
    }
    Txn.SelectFlowRequest.Builder builder = isPrimary ? buildTableReaderReqForKeys(processors, outputOffsetList, keys)
                                                : buildIndexReaderReqForKeys(processors, outputOffsetList, keys);
    return MULTI_KEY_SELECT_FUNC.apply(shardSender, storeCtx, builder, resultColumns, keys)
               .onErrorResume(MultiKeySelector.getErrHandler(shardSender, storeCtx, MULTI_KEY_SELECT_FUNC, builder, resultColumns, keys));
  }

  private static Txn.SelectFlowRequest.Builder buildTableReaderReqForKeys(List<Processorpb.Processor.Builder> processors,
                                                                          List<Integer> outputOffsetList, List<ByteString> keys) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.TABLE_READ_TYPE) {
        Processorpb.TableRead.Builder tableRead = Processorpb.TableRead.newBuilder(processor.getTableRead())
                                                      .setType(Processorpb.KeyType.PRIMARY_KEY_TYPE)
                                                      .addAllPkKeys(keys);
        processor.setTableRead(tableRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private static Txn.SelectFlowRequest.Builder buildIndexReaderReqForKeys(List<Processorpb.Processor.Builder> processors,
                                                                          List<Integer> outputOffsetList, List<ByteString> keys) {
    Txn.SelectFlowRequest.Builder builder = Txn.SelectFlowRequest.newBuilder();
    for (Processorpb.Processor.Builder processor : processors) {
      if (processor.getType() == Processorpb.ProcessorType.INDEX_READ_TYPE) {
        Processorpb.IndexRead.Builder indexRead = Processorpb.IndexRead.newBuilder(processor.getIndexRead())
                                                      .setType(Processorpb.KeyType.PRIMARY_KEY_TYPE)
                                                      .addAllIndexKeys(keys);
        processor.setIndexRead(indexRead);
      }
      builder.addProcessors(processor);
    }
    builder.addAllOutputOffsets(outputOffsetList).setGatherTrace(GATHER_TRACE);
    return builder;
  }

  private static Flux<ExecResult> convertToExecResult(ColumnExpr[] resultColumns, Flux<ValueAccessor[]> valuesFlux) {
    if (valuesFlux == null) {
      return Flux.create(sink -> sink.next(new QueryExecResult(resultColumns, ROW_VALUE_ACCESSORS_EMPTY)));
    }
    return valuesFlux.map(rows -> {
      if (rows == null || rows.length == 0) {
        return new QueryExecResult(resultColumns, ROW_VALUE_ACCESSORS_EMPTY);
      }
      return new QueryExecResult(resultColumns, rows);
    });
  }

  // sendSelectReqFunc
  private static Flux<List<Txn.Row>> sendSelectReq(ShardSender shardSender, StoreCtx storeCtx, Txn.SelectFlowRequest.Builder reqBuilder,
                                                   ByteString key,
                                                   RangeInfo rangeInfo) {
    RequestContext context = new RequestContext(storeCtx, key, rangeInfo, reqBuilder, Api.RangeRequest.ReqCase.SELECT_FLOW);

    return shardSender.sendReq(context).map(response -> {
      Txn.SelectFlowResponse selectResponse = (Txn.SelectFlowResponse) response;
      if (selectResponse.getCode() > 0) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_SHARD_RESPONSE_CODE, "sendSelectReq",
            String.valueOf(selectResponse.getCode()));
      }
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("txn select response success: response row {}", selectResponse.getRowsList().size());
      }
//      LOG.error("txn select response success: response row {}", selectResponse.getTracesList());
      return selectResponse.getRowsList();
    });
  }


}
