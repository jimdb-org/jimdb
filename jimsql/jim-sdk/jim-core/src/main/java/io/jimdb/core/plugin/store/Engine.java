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
package io.jimdb.core.plugin.store;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.jimdb.core.Session;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.plugin.Plugin;
import io.jimdb.core.context.ReorgContext;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Statspb;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
public interface Engine extends Plugin {
  Transaction beginTxn(Session session);

  Flux<Boolean> put(Table table, byte[] key, byte[] value, Instant timeout) throws JimException;

  Flux<byte[]> get(Table table, byte[] key, Instant timeout) throws JimException;

  Flux<Boolean> delete(Table table, byte[] key, Instant timeout) throws JimException;

  Set<RangeInfo> getRanges(Table table) throws JimException;

  void reOrganize(ReorgContext context, Table table, Index index, Ddlpb.OpType opType) throws JimException;

  Flux<List<Tuple2<Statspb.Histogram, Statspb.CMSketch>>> analyzeIndex(Table table, Index index, Instant timeout, List<ValueRange> ranges,
                                                                       Statspb.IndexStatsRequest.Builder reqBuilder) throws JimException;

  Flux<List<Tuple2<Statspb.Histogram, List<Statspb.SampleCollector>>>> analyzeColumns(Table table, Column[] columns, Instant timeout, List<ValueRange> ranges,
                                                                                      Statspb.ColumnsStatsRequest.Builder reqBuilder) throws JimException;
}
