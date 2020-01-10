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
package io.jimdb.core.codec;

/**
 * @version 1.0
 */
public final class DecodeTag {
  private int typeOffset;
  private int dataOffset;
  private int length;
  private ValueCodec.TagType type;

  public DecodeTag(int typeOffset, int length) {
    this.typeOffset = typeOffset;
    this.length = length;
  }

  public DecodeTag(int typeOffset, int dataOffset, ValueCodec.TagType type) {
    this.typeOffset = typeOffset;
    this.dataOffset = dataOffset;
    this.type = type;
  }

  public int getTypeOffset() {
    return typeOffset;
  }

  public int getDataOffset() {
    return dataOffset;
  }

  public ValueCodec.TagType getType() {
    return type;
  }

  public void setType(ValueCodec.TagType type) {
    this.type = type;
  }

  public int getLength() {
    return length;
  }
}
