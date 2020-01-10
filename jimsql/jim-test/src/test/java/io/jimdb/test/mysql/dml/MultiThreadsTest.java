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
package io.jimdb.test.mysql.dml;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.jimdb.test.mysql.SqlTestBase;

import org.junit.Test;

/**
 * @version V1.0
 */
public class MultiThreadsTest extends SqlTestBase {


  @Test
  public void testMultiThread() {

    ExecutorService executorService = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 10000; i++) {
      int finalI = i;
      executorService.submit(() -> {
        try {
          execQueryNotCheck("select sum(num_double) as sum,avg(num_double) as avg,count(1) as cnt,max(num_double) as max,min(num_double) as min,country "
                                    + "from baker02 "
                  + "where age = " + (finalI % 100 + 1)
                  + " group by country");

        } catch (Exception e) {
          e.printStackTrace();
        }

      });
    }

    try {
      executorService.awaitTermination(100000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
