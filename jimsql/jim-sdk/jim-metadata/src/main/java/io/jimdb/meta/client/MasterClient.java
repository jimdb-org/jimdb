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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import io.jimdb.core.config.JimConfig;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.core.model.meta.Table;
import io.jimdb.pb.Mspb;
import io.jimdb.core.plugin.RouterStore;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * master http meta client
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CHECKED", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS" })
public final class MasterClient implements RouterStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterClient.class);

  private static final String RANGE_CREATE = "/range/create";
  private static final String RANGE_DELETE = "/range/delete";
  private static final String ROUTE_GET = "/range/routers";

  private static final String NODE_GET = "/node/get";
  private static final String TABLE_STAT_COUNT = "/table/count";

  private String url;
  private long clusterId;

  private HttpClientManager httpClient;

  public MasterClient() {

  }

  //for test
  public MasterClient(String url, long clusterId) {
    this.url = url;
    this.clusterId = clusterId;
    httpClient = new HttpClientManager(new HttpClientManager.ClientConfig());
  }

  @Override
  public void init(JimConfig c) {
    this.url = c.getMasterAddr();
    this.clusterId = c.getMetaCluster();
    if (this.httpClient == null) {
      this.httpClient = new HttpClientManager(new HttpClientManager.ClientConfig());
    }
  }

  @Override
  public void createRange(Mspb.CreateRangesRequest request) {
    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    request = request.toBuilder().setHeader(header).build();
    byte[] content = request.toByteArray();

    try {
      byte[] data = postWithPb(url + RANGE_CREATE, content);
      Mspb.GeneralResponse generalResponse = Mspb.GeneralResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = generalResponse.getHeader();
      if (responseHeader.hasError()) {
        Mspb.Error error = responseHeader.getError();
        throw DBException.get(ErrorModule.META, ErrorCode.ER_META_CREATE_RANGE,
                String.valueOf(request.getTableId()), String.valueOf(error.getCode()), error.getMessage());
      }
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_CREATE_RANGE, ex,
              String.valueOf(request.getTableId()), "0", ex.getMessage());
    }
  }

  @Override
  public void deleteRange(Mspb.DeleteRangesRequest request) {
    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    request = request.toBuilder().setHeader(header).build();
    byte[] content = request.toByteArray();

    try {
      byte[] data = postWithPb(url + RANGE_DELETE, content);
      Mspb.GeneralResponse generalResponse = Mspb.GeneralResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = generalResponse.getHeader();
      if (responseHeader.hasError()) {
        Mspb.Error error = responseHeader.getError();
        throw DBException.get(ErrorModule.META, ErrorCode.ER_META_DELETE_RANGE,
                String.valueOf(request.getTableId()), String.valueOf(error.getCode()), error.getMessage());
      }
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_DELETE_RANGE, ex,
              String.valueOf(request.getTableId()), "0", ex.getMessage());
    }
  }

  /******************************************************************************
   route api
   ******************************************************************************/

  @Override
  public Mspb.GetRouteResponse getRoute(long dbId, long tableId, byte[] key, int retry) {
    if (retry < 0) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_ROUTE, String.valueOf(tableId));
    }
    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    Mspb.GetRouteRequest.Builder getRouteRequest = Mspb.GetRouteRequest.newBuilder()
            .setHeader(header)
            .setDbId((int) dbId)
            .setTableId((int) tableId)
            .setMax(10);

    if (key != null && key.length != 0) {
      getRouteRequest.setKey(ByteString.copyFrom(key));
    }
    byte[] content = getRouteRequest.build().toByteArray();
    try {
      byte[] data = postWithPb(url + ROUTE_GET, content);
      Mspb.GetRouteResponse getRouteResponse = Mspb.GetRouteResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = getRouteResponse.getHeader();
      if (!responseHeader.hasError()) {
        return getRouteResponse;
      }
      handleHeaderError(responseHeader.getError(), dbId, tableId, 0);

      return getRoute(dbId, tableId, key, --retry);
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_ROUTE, ex, String.valueOf(tableId));
    }
  }

  private void handleHeaderError(Mspb.Error pbErr, long dbId, long tableId, long nodeId) {
    int code = pbErr.getCode();
    switch (code) {
      case Mspb.ErrorType.NotExistDatabase_VALUE:
        throw MetaException.get(ErrorCode.ER_BAD_DB_ERROR, String.valueOf(dbId));
      case Mspb.ErrorType.NotExistTable_VALUE:
        throw MetaException.get(ErrorCode.ER_BAD_TABLE_ERROR, String.valueOf(tableId));
      case Mspb.ErrorType.NotExistNode_VALUE:
        throw MetaException.get(ErrorCode.ER_META_NODE_NOT_EXIST, String.valueOf(nodeId));
      case Mspb.ErrorType.NotExistRange_VALUE:
        throw MetaException.get(ErrorCode.ER_META_SHARD_NOT_EXIST);
      default:
        LOGGER.error("get meta from remote error:{} ", pbErr);
        return;
    }
  }

  /******************************************************************************
   node api
   ******************************************************************************/

  @Override
  public Mspb.GetNodeResponse getNode(long id, int retry) {
    if (retry < 0) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_NODE, String.valueOf(id));
    }
    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    Mspb.GetNodeRequest getNodeRequest = Mspb.GetNodeRequest.newBuilder().setHeader(header).setId(id).build();
    byte[] content = getNodeRequest.toByteArray();
    try {
      byte[] data = postWithPb(url + NODE_GET, content);
      Mspb.GetNodeResponse getNodeResponse = Mspb.GetNodeResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = getNodeResponse.getHeader();
      if (!responseHeader.hasError()) {
        return getNodeResponse;
      }
      handleHeaderError(responseHeader.getError(), 0, 0, id);
      return getNode(id, --retry);
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_NODE, String.valueOf(id));
    }
  }

  /**
   * Obtain the row counts for all the tables from the master
   *
   * @param retry number of retries
   * @return row counts of all tables
   */
  public Mspb.CountTableResponse getAllTableStats(int retry) {
    if (retry < 0) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_TABLE_STATS);
    }
    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    Mspb.CountTableRequest getTableStatsRequest = Mspb.CountTableRequest.newBuilder()
            .setHeader(header)
            .build();
    byte[] content = getTableStatsRequest.toByteArray();
    try {
      byte[] data = postWithPb(url + TABLE_STAT_COUNT, content);
      Mspb.CountTableResponse getTableStatsResponse = Mspb.CountTableResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = getTableStatsResponse.getHeader();
      if (!responseHeader.hasError()) {
        return getTableStatsResponse;
      }
      handleHeaderError(responseHeader.getError(), 0, 0, 0);

      return getAllTableStats(--retry);
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_TABLE_STATS);
    }
  }

  /**
   * Obtain the row counts for a set of given tables from the master
   *
   * @param tables table to query
   * @param retry  number of reties
   * @return row counts of the given tables
   */
  public Mspb.CountTableResponse getTableStats(Set<Table> tables, int retry) {
    if (retry < 0) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_TABLE_STATS);
    }

    Set<Mspb.Counter> pbTables = tables.stream()
            .map(table -> Mspb.Counter.newBuilder()
                    .setDbId(table.getCatalogId())
                    .setTableId(table.getId())
                    .build())
            .collect(Collectors.toSet());

    Mspb.RequestHeader header = Mspb.RequestHeader.newBuilder().setClusterId(clusterId).build();
    Mspb.CountTableRequest getTableStatsRequest = Mspb.CountTableRequest.newBuilder()
            .setHeader(header)
            .addAllTables(pbTables)
            .build();
    byte[] content = getTableStatsRequest.toByteArray();
    try {
      byte[] data = postWithPb(url + TABLE_STAT_COUNT, content);
      Mspb.CountTableResponse getTableStatsResponse = Mspb.CountTableResponse.parseFrom(data);
      Mspb.ResponseHeader responseHeader = getTableStatsResponse.getHeader();
      if (!responseHeader.hasError()) {
        return getTableStatsResponse;
      }
      handleHeaderError(responseHeader.getError(), 0, 0, 0);

      return getTableStats(tables, --retry);
    } catch (IOException ex) {
      throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_TABLE_LIST);
    }
  }

  private byte[] postWithPb(String url, byte[] content) throws IOException {
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-Type", "application/proto; charset=utf-8");
    httpPost.setEntity(new ByteArrayEntity(content));
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      HttpEntity entity2 = response.getEntity();
      entity2.writeTo(buf);
      return buf.toByteArray();
    } catch (IOException ex) {
      throw ex;
    } finally {
      buf.close();
      httpPost.abort();
    }
  }

  @Override
  public void close() {
    httpClient.close();
  }
}
