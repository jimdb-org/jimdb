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
package io.jimdb.core.codec;

import io.jimdb.common.utils.lang.ByteUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

/**
 * @version V1.0
 */
public final class KvPair {
  private ByteString key;
  private ByteString value;
  private boolean isKey;

  public KvPair(ByteString key) {
    this(key, null);
  }

  public KvPair(ByteString key, ByteString value) {
    this.key = key;
    this.value = value;
  }

  public KvPair(ByteString key, ByteString value, boolean isKey) {
    this.key = key;
    this.value = value;
    this.isKey = isKey;
  }

  public ByteString getKey() {
    return key;
  }

  public void setKey(ByteString key) {
    this.key = key;
  }

  public ByteString getValue() {
    return value;
  }

  public void setValue(ByteString value) {
    this.value = value;
  }

  public boolean isKey() {
    return isKey;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("KvPair{");
    sb.append("key=").append(ByteUtil.bytes2hex01(NettyByteString.asByteArray(key)));
    if (value != null) {
      sb.append(", value=").append(ByteUtil.bytes2hex01(NettyByteString.asByteArray(value)));
    }
    sb.append('}');
    return sb.toString();
  }
}
