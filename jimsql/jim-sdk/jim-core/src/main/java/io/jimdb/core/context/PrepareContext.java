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
package io.jimdb.core.context;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.model.prepare.JimStatement;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Metapb;
import io.jimdb.common.utils.lang.Resetable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @version V1.0
 */
public class PrepareContext implements Resetable {

  private volatile int prepareId;
  private boolean useCache;
  private volatile Cache<Object, Object> prepareCache;
  private ConcurrentHashMap<Integer, JimStatement> jimStatementMap;
  private ConcurrentHashMap<String, Integer> preparedNameIdMap; //for console
  private List<Value> preparedParams;
  private List<Metapb.SQLType> prepareTypes;


  public PrepareContext() {
    this.jimStatementMap = new ConcurrentHashMap<>();
    this.preparedNameIdMap = new ConcurrentHashMap<>();
    this.prepareCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumWeight(1024L * 1024 * 1024).weigher((o, o2) -> o.toString().length() + o2.toString().length()).build();
  }

  public int getNextPreparedStmtID() {
    return ++prepareId;
  }

  public ConcurrentHashMap<Integer, JimStatement> getPreparedStmts() {
    return jimStatementMap;
  }

  public void setPreparedStmts(ConcurrentHashMap<Integer, JimStatement> preparedStmts) {
    this.jimStatementMap = preparedStmts;
  }

  public ConcurrentHashMap<String, Integer> getPreparedNameIdMap() {
    return preparedNameIdMap;
  }

  public void setPreparedNameIdMap(ConcurrentHashMap<String, Integer> preparedNameIdMap) {
    this.preparedNameIdMap = preparedNameIdMap;
  }

  public List<Value> getPreparedParams() {
    return preparedParams;
  }

  public List<Metapb.SQLType> getPrepareTypes() {
    return prepareTypes;
  }

  public void setPrepareTypes(List<Metapb.SQLType> prepareTypes) {
    this.prepareTypes = prepareTypes;
  }

  public void setPreparedParams(List<Value> preparedParams) {
    this.preparedParams = preparedParams;
  }

  public Cache getCache() {
    return prepareCache;
  }

  public void setCache(Cache cache) {
    this.prepareCache = cache;
  }

  public boolean isUseCache() {
    return useCache;
  }

  public void setUseCache(boolean useCache) {
    this.useCache = useCache;
  }

  @Override
  public void reset() {
    preparedParams = null;
  }

  @Override
  public void close() {
    if (this.prepareCache != null) {
      this.prepareCache.cleanUp();
    }
  }
}
