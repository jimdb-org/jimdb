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
package io.jimdb.mysql.prepare;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EI_EXPOSE_REP", "UP_UNUSED_PARAMETER" })
public class NullBitMap {

  private int offset;

  private int[] bits;

  public NullBitMap(int offset, int fieldsNum) {
    this.offset = offset;
    this.bits = initCapacity(offset, fieldsNum);
  }

  /**
   * NULL-bitmap-bytes = (num-fields + 7 + offset) / 8
   *
   * @param fieldsNum
   * @param offset
   * @return
   */
  @SuppressFBWarnings("SUA_SUSPICIOUS_UNINITIALIZED_ARRAY")
  private int[] initCapacity(final int fieldsNum, final int offset) {
    return new int[(fieldsNum + 9) / 8];
  }


  private int getBitPosition(final int pos) {
    return (pos + offset) % 8;
  }

  public boolean isNull(final int index) {
    return (bits[getBytePosition(index)] & (1 << getBitPosition(index))) != 0;
  }


  public void setNullBit(final int index) {
    bits[getBytePosition(index)] |= 1 << getBitPosition(index);
  }

  private int getBytePosition(final int index) {
    return (index + offset) / 8;
  }

  public int getOffset() {
    return offset;
  }

  public int[] getBits() {
    return bits;
  }
}
