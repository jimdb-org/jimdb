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

import io.jimdb.common.utils.event.Event;
import io.jimdb.common.utils.event.EventListener;

/**
 * @version 1.0
 */
public class TEventListener implements EventListener {
  String lastEventName;

  @Override
  public String getName() {
    return "test-event-listener";
  }

  @Override
  public boolean isSupport(Event e) {
    return e instanceof TEventOne;
  }

  @Override
  public void handleEvent(Event e) {
    lastEventName = e.getName();
  }

  public String getLastEventName() {
    return lastEventName;
  }
}
