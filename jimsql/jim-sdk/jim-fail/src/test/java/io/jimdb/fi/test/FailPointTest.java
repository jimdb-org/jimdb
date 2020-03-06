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
package io.jimdb.fi.test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.BaseException;
import io.jimdb.fi.FailPoint;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @version V1.0
 */
public final class FailPointTest {
  @BeforeClass
  public static void init() {
    System.setProperty("fi.config", "fi_config.properties");

    testSetup();
  }

  static void testSetup() {
    Map<String, String> faults = FailPoint.registers();
    Assert.assertEquals(3, faults.size());
    Assert.assertEquals("return", faults.get("fi.test1"));
    Assert.assertEquals("50%100*return(1)->90%100*return(-1)->50*print", faults.get("fi.test2"));
    Assert.assertEquals("50%sleep(1000)->print(test success)", faults.get("fi.test3"));

    FailPoint.remove("fi.test2");
    faults = FailPoint.registers();
    Assert.assertEquals(2, faults.size());
    Assert.assertEquals("return", faults.get("fi.test1"));
    Assert.assertEquals("50%sleep(1000)->print(test success)", faults.get("fi.test3"));

    FailPoint.clear();
    faults = FailPoint.registers();
    Assert.assertEquals(0, faults.size());
  }

  @Test
  public void testEnable() {
    Assert.assertEquals(true, FailPoint.isEnable());
  }

  @Test
  public void testMultiAction() {
    Function<Boolean, Integer> func = arg -> {
      if (FailPoint.isEnable()) {
        Integer rs = FailPoint.inject("fi.multi-test", () -> arg, v -> StringUtils.isBlank(v) ? 10 : new Integer(v));
        if (rs != null) {
          return rs;
        }
      }
      return 0;
    };

    FailPoint.config("fi.multi-test", "50%100*return(1)->90%100*return(-1)->50*return");
    int sum = 0;
    for (int i = 0; i < 10000; i++) {
      sum += func.apply(true);
    }
    Assert.assertEquals(500, sum);
  }

  @Test
  public void testOff() {
    Function<String, String> func = arg -> {
      if (FailPoint.isEnable()) {
        String rs = FailPoint.inject("fi.off-test", v -> "off-failed");
        if (rs != null) {
          return rs;
        }
      }

      return arg;
    };

    Assert.assertEquals("hello", func.apply("hello"));
    FailPoint.config("fi.off-test", "off");
    Assert.assertEquals("hello", func.apply("hello"));
  }

  @Test
  public void testReturn() {
    Function<String, String> func = arg -> {
      String rs = FailPoint.inject("fi.return-test", v -> v);
      return rs == null ? arg : rs;
    };

    Assert.assertEquals("hello", func.apply("hello"));
    FailPoint.config("fi.return-test", "return(return-value)");
    Assert.assertEquals("return-value", func.apply("hello"));

    FailPoint.config("fi.return-test", "return");
    Assert.assertEquals("", func.apply("hello"));
  }

  @Test
  public void testSleep() {
    Function<String, String> func = arg -> {
      FailPoint.inject("fi.sleep-test");
      return arg;
    };

    long start = System.currentTimeMillis();
    Assert.assertEquals("hello", func.apply("hello"));
    Assert.assertTrue(System.currentTimeMillis() - start < 1000);

    FailPoint.config("fi.sleep-test", "sleep(1000)");
    start = System.currentTimeMillis();
    Assert.assertEquals("hello", func.apply("hello"));
    Assert.assertTrue(System.currentTimeMillis() - start >= 1000);
  }

  @Test
  public void testPanic() {
    Function<String, String> func = arg -> {
      FailPoint.inject("fi.panic-test");
      return arg;
    };

    Assert.assertEquals("hello", func.apply("hello"));
    FailPoint.config("fi.panic-test", "panic(ER_DB_CREATE_EXISTS;dbTest)");

    ErrorCode code = ErrorCode.ER_UNKNOWN_ERROR;
    String msg = "";
    try {
      func.apply("hello");
    } catch (BaseException ex) {
      code = ex.getCode();
      msg = ex.getMessage();
    }

    Assert.assertEquals(ErrorCode.ER_DB_CREATE_EXISTS, code);
    Assert.assertEquals("Can't create database 'dbTest'; database exists", msg);
  }

  @Test
  public void testPause() throws InterruptedException {
    final AtomicInteger counter = new AtomicInteger(1);
    Consumer<AtomicInteger> func = arg -> {
      FailPoint.inject("fi.pause-test");
      arg.incrementAndGet();
    };

    func.accept(counter);
    Assert.assertEquals(2, counter.get());

    FailPoint.config("fi.pause-test", "pause");

    new Thread(() -> {
      func.accept(counter);
      func.accept(counter);
      func.accept(counter);
    }).start();

    Thread.sleep(2000);
    Assert.assertEquals(2, counter.get());

    FailPoint.config("fi.pause-test", "pause");
    Thread.sleep(2000);
    Assert.assertEquals(3, counter.get());

    FailPoint.remove("fi.pause-test");
    Thread.sleep(2000);
    Assert.assertEquals(5, counter.get());
  }

  @Test
  public void testYield() {
    Function<String, String> func = arg -> {
      FailPoint.inject("fi.yield-test");
      return arg;
    };

    FailPoint.config("fi.yield-test", "yield");
    Assert.assertEquals("hello", func.apply("hello"));
  }

  @Test
  public void testDelay() {
    Function<String, String> func = arg -> {
      FailPoint.inject("fi.delay-test");
      return arg;
    };

    long start = System.currentTimeMillis();
    Assert.assertEquals("hello", func.apply("hello"));
    Assert.assertTrue(System.currentTimeMillis() - start < 1000);

    FailPoint.config("fi.delay-test", "delay(1000)");
    start = System.currentTimeMillis();
    Assert.assertEquals("hello", func.apply("hello"));
    Assert.assertTrue(System.currentTimeMillis() - start >= 1000);
  }

  @Test
  public void testPredicate() {
    Function<Boolean, Integer> func = arg -> {
      Integer rs = FailPoint.inject("fi.predicate-test", () -> arg, v -> new Integer(v));
      return rs == null ? 1 : rs;
    };

    Assert.assertEquals(1, func.apply(true).intValue());
    FailPoint.config("fi.predicate-test", "return(1000)");
    Assert.assertEquals(1, func.apply(false).intValue());
    Assert.assertEquals(1000, func.apply(true).intValue());
  }
}
