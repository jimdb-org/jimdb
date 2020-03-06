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
package io.jimdb.common.utils.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler that will be invoked when a thread abruptly terminates due to an uncaught exception.
 *
 */
public final class UncaughtExceptionHandlerImpl implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptionHandlerImpl.class);
  private static final UncaughtExceptionHandlerImpl INSTANCE = new UncaughtExceptionHandlerImpl();

  private UncaughtExceptionHandlerImpl() {
  }

  public static UncaughtExceptionHandlerImpl getInstance() {
    return INSTANCE;
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    if (isFatalUncaught(e)) {
      LOG.error(String.format("fatal error found in thread[%s], exiting.", t.getName()), e);
    } else {
      LOG.error(String.format("uncaught exception found in thread[%s].", t.getName()), e);
    }
  }

  private boolean isFatalUncaught(Throwable e) {
    return e instanceof Error;
  }
}
