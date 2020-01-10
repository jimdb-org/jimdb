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
package io.jimdb.core.plugin.rpc;

/**
 * Callback function of the request command.
 *
 * @param <T>
 */
public abstract class CommandCallback<T extends Command> {

  /**
   * Success callback.
   *
   * @param request
   * @param response
   */
  public final void onSuccess(final T request, final T response) {
    boolean isRelease = onSuccess0(request, response);

    if (isRelease && request != null) {
      request.close();
    }
    if (isRelease && response != null) {
      response.close();
    }
  }

  /**
   * If request and response can release, then return true;
   *
   * @param request
   * @param response
   * @return
   */
  protected abstract boolean onSuccess0(T request, T response);

  /**
   * Exception callback.
   *
   * @param request
   * @param cause
   */
  public final void onFailed(final T request, final Throwable cause) {
    boolean isRelease = onFailed0(request, cause);

    if (isRelease && request != null) {
      request.close();
    }
  }

  /**
   * If request can release, then return true;
   *
   * @param request
   * @param cause
   * @return
   */
  protected abstract boolean onFailed0(T request, Throwable cause);
}
