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
package io.jimdb.meta;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.jimdb.core.config.JimConfig;
import io.jimdb.meta.client.EtcdClient;
import io.jimdb.common.utils.lang.IOUtil;
import io.etcd.jetcd.ByteSequence;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class EtcdMetaStoreInitTest {
  private static JimConfig config;

  @BeforeClass
  public static void beforeClass() {
    Properties properties = IOUtil.loadResource("jim.properties");
    config = new JimConfig(properties);
  }

  @Test
  public void testInit() throws Exception {
    EtcdClient client = EtcdClient.getInstance(config.getMetaStoreAddress(), 2 * config.getMetaLease(), TimeUnit.MILLISECONDS);
    client.delete(EtcdMetaStore.SCHEMA_ROOT);

    int parallel = 32;
    CountDownLatch latch = new CountDownLatch(parallel);
    for (int i = 0; i < parallel; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          EtcdMetaStore store = new EtcdMetaStore();
          store.init(config);
          latch.countDown();
        }
      };
      t.start();
    }

    latch.await();
    ByteSequence value = client.get(EtcdMetaStore.INIT_PATH);
    Assert.assertNotNull(value);
    Assert.assertEquals("Initialized", value.toString(StandardCharsets.UTF_8));

    client.delete(EtcdMetaStore.SCHEMA_ROOT);
  }
}
