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
package io.jimdb.test.utils.event;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.jimdb.common.utils.event.EventBus;

import org.junit.Assert;
import org.junit.Test;

/**
 * @version V1.0
 */
public class EventBusTest {

  @Test
  public void test() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    TEventListener listener = new TEventListener();
    EventBus eventBus = new EventBus("test-eventbus", executorService);
    eventBus.addListener(listener);

    TEventOne one = new TEventOne("one", 1);
    TEventTwo two = new TEventTwo("two", 2);

    eventBus.publish(one);
    eventBus.publish(two);

    try {
      // waiting for asynchronous event to handle
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(one.getName(), listener.getLastEventName());
  }
}
