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
package io.jimdb.meta.route;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

/**
 * least recently used
 *
 * @param <K>
 * @param <V>
 * @version 1.0
 */
public class LRUCache<K, V> {

  //delegated MAP
  private final Map<K, V> delegate;

  //used to store keys that are eliminated by LRU policies
  private ConcurrentMap<K, V> keyMap;

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  private final Lock r = rwl.readLock();

  private final Lock w = rwl.writeLock();

  protected LRUCache(Map<K, V> delegate) {
    this.delegate = delegate;
    setSize(1024);
  }

  protected LRUCache(Map<K, V> delegate, int size) {
    this.delegate = delegate;
    setSize(size);
  }

  /**
   * 构建给定大小，按访问顺序排序的ConcurrentLinkedHashMap
   *
   * @param size
   */
  private void setSize(final int size) {
    keyMap = new ConcurrentLinkedHashMap.Builder<K, V>().maximumWeightedCapacity(size).listener(new EvictionListener<K, V>() {
      @Override
      public void onEviction(K key, V value) {
        w.lock();
        try {
          delegate.remove(value);
          releaseResource(value);
        } catch (Exception ignore) {

        } finally {
          w.unlock();
        }
      }
    }).build();
  }

  /**
   * 写入缓存
   *
   * @param key
   * @param value
   */
  protected void putObject(K key, V value) {
    if (key == null || value == null) {
      return;
    }
    w.lock();
    try {
      delegate.put(key, value);
      keyMap.put(key, value);
    } finally {
      w.unlock();
    }
  }

  /**
   * 获取缓存
   *
   * @param key
   * @return
   */
  protected V getObject(K key) {
    if (key == null) {
      return null;
    }
    r.lock();
    try {
      keyMap.get(key); // 更新访问顺序
      return delegate.get(key);
    } finally {
      r.unlock();
    }
  }

  protected V getObjectNoStat(K key) {
    if (key == null) {
      return null;
    }
    r.lock();
    try {
      return delegate.get(key);
    } finally {
      r.unlock();
    }
  }

  /**
   * 移除缓存
   *
   * @param key
   * @return
   */
  protected V removeObject(K key) {
    if (key == null) {
      return null;
    }
    w.lock();
    try {
      keyMap.remove(key);
      return delegate.remove(key);
    } finally {
      w.unlock();
    }
  }

  /**
   * 清空缓存
   */
  protected void clearCache() {
    w.lock();
    try {
      delegate.clear();
      keyMap.clear();
    } finally {
      w.unlock();
    }
  }

  /**
   * 释放被淘汰元素锁占用的资源
   *
   * @param value
   */
  protected void releaseResource(V value) {
  }

  protected Map<K, V> getDelegate() {
    return delegate;
  }
}
