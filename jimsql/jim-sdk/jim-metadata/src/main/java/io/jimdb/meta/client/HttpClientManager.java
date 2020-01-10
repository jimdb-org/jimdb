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
package io.jimdb.meta.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

/**
 * @version V1.0
 */
final class HttpClientManager {
  private final Object lockObj;
  private final ClientConfig config;
  private CloseableHttpClient httpClient;
  private IdleConnectionMonitorThread monitor;
  private PoolingHttpClientConnectionManager connectionManager;

  HttpClientManager(final ClientConfig config) {
    this.lockObj = new Object();
    this.config = config;
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(config.getMaxTotal());
    connectionManager.setDefaultMaxPerRoute(config.getMaxPerRoute());
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(config.getConnectTimeout())
            .setSocketTimeout(config.getSocketTimeout()).setConnectionRequestTimeout(config.getRequestTimeout())
            .setRedirectsEnabled(config.isRedirect()).build();
    DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(config.getRetryCount(), true);
    httpClient = HttpClientBuilder.create().setConnectionManager(connectionManager)
            .setRetryHandler(retryHandler).setDefaultRequestConfig(requestConfig).build();
    monitor = new IdleConnectionMonitorThread();
    monitor.setDaemon(true);
    monitor.start();
  }

  CloseableHttpResponse execute(HttpUriRequest request) throws IOException {
    return httpClient.execute(request, (HttpContext) null);
  }

  /**
   * 关闭
   */
  synchronized void close() {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException e) {
      }
    }
    if (connectionManager != null) {
      connectionManager.close();
    }
    if (monitor != null) {
      monitor.close();
    }
  }

  /**
   * 管理空闲的连接
   */
  class IdleConnectionMonitorThread extends Thread {
    private volatile boolean closed;

    @Override
    public void run() {
      try {
        while (!closed) {
          synchronized (lockObj) {
            lockObj.wait(config.getIdleInterval());
            // Close expired connections
            connectionManager.closeExpiredConnections();
            // Optionally, close idle connections
            if (config.getMaxIdleTime() > 0) {
              connectionManager.closeIdleConnections(config.getMaxIdleTime(), TimeUnit.MILLISECONDS);
            }
          }
        }
      } catch (InterruptedException ex) {
        // terminate
      }
    }

    public void close() {
      closed = true;
      synchronized (lockObj) {
        lockObj.notifyAll();
      }
    }
  }

  /**
   * @version V1.0
   */
  static final class ClientConfig {
    private int maxTotal = 50;
    private int maxPerRoute = 30;
    private int connectTimeout = 5000;
    private int requestTimeout = 6000;
    private int socketTimeout = 10000;
    private int maxIdleTime = 10 * 60 * 1000;
    private int idleInterval = 10 * 60 * 1000;
    private int retryCount = 1;
    private boolean redirect = false;

    public int getMaxTotal() {
      return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
      this.maxTotal = maxTotal;
    }

    public int getMaxPerRoute() {
      return maxPerRoute;
    }

    public void setMaxPerRoute(int maxPerRoute) {
      this.maxPerRoute = maxPerRoute;
    }

    public int getConnectTimeout() {
      return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
    }

    public int getRequestTimeout() {
      return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
      this.requestTimeout = requestTimeout;
    }

    public int getSocketTimeout() {
      return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
    }

    public int getMaxIdleTime() {
      return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
    }

    public int getIdleInterval() {
      return idleInterval;
    }

    public void setIdleInterval(int idleInterval) {
      this.idleInterval = idleInterval;
    }

    public int getRetryCount() {
      return retryCount;
    }

    public void setRetryCount(int retryCount) {
      this.retryCount = retryCount;
    }

    public boolean isRedirect() {
      return redirect;
    }

    public void setRedirect(boolean redirect) {
      this.redirect = redirect;
    }
  }
}
