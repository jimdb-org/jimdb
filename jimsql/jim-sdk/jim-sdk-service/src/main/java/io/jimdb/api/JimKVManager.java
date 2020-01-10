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
package io.jimdb.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.jimdb.engine.JimStoreEngine;
import io.jimdb.core.model.meta.Catalog;
import io.jimdb.core.model.meta.Table;
import io.jimdb.common.utils.os.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public class JimKVManager {
  private static final Logger LOG = LoggerFactory.getLogger(JimKVManager.class);

  private JimStoreEngine storeEngine;
  private Catalog db;

  public JimKVManager(Catalog db, JimStoreEngine storeEngine) {
    this.db = db;
    this.storeEngine = storeEngine;
  }

  public void put(String tableName, byte[] key, byte[] value) {
    Table table = db.getTable(tableName);
    Instant timeout = SystemClock.currentTimeStamp().plusMillis(100);
    storeEngine.put(table, key, value, timeout).blockFirst();
  }

  public byte[] get(String tableName, byte[] key) {
    final List<byte[]> record = new ArrayList<>(1);
    try {
      Table table = db.getTable(tableName);
      Instant timeout = SystemClock.currentTimeStamp().plusMillis(100);
      Flux<byte[]> flux = storeEngine.get(table, key, timeout);
      CountDownLatch latch = new CountDownLatch(1);
      flux.subscribe(m -> {
        try {
          record.add(m);
        } finally {
          latch.countDown();
        }
      }, e -> {
        latch.countDown();
      });

      latch.await(100, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      return record.get(0);
    }
  }

  public void delete(String tableName, byte[] key) {
    Table table = db.getTable(tableName);
    Instant timeout = SystemClock.currentTimeStamp().plusMillis(100);
    storeEngine.delete(table, key, timeout).blockFirst();
  }
}
