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
package io.jimdb.test.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import io.jimdb.common.utils.generator.UUIDGenerator;

import org.junit.Test;

/**
 * @version V1.0
 */
public class UUIDTest {
  private static final int THREAD_NUM = Math.max(8, Runtime.getRuntime().availableProcessors());
  private static final ConcurrentHashMap<String, String> UUIDSMAP = new ConcurrentHashMap<>(204800, 1.0f);

  private final CountDownLatch latch = new CountDownLatch(THREAD_NUM);

  @Test
  public void uuidTest() throws Exception {
    final int num = 3000000;
    for (int i = 0; i < THREAD_NUM; i++) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < num; j++) {
            String uid = UUIDGenerator.next();
            uid = UUIDSMAP.putIfAbsent(uid, uid);
            if (uid != null) {
              System.out.println("UUID is not unique.");
              System.exit(1);
            }
          }
          latch.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    latch.await();
  }
}
