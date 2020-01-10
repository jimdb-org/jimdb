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

package io.jimdb.sql.optimizer.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.jimdb.pb.Statspb;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * FMSketch is used to count the number of distinct elements in a set.
 */
@SuppressFBWarnings({ "UUF_UNUSED_FIELD", "UWF_UNWRITTEN_FIELD",
        "URF_UNREAD_FIELD", "NP_UNWRITTEN_FIELD" })
public class FlajoletMartinSketch {

  private Set<Long> valueSet;
  private long mask;
  private int maxSize;


  public FlajoletMartinSketch(int maxSize) {
    this.maxSize = maxSize;
    this.valueSet = Sets.newHashSet();
    this.mask = 0;
  }

  private FlajoletMartinSketch(int maxSize, long mask) {
    this(maxSize);
    this.mask = mask;
  }

  public long getNDV() {
    return (long) (maxSize + 1) * valueSet.size();
  }

  public void insertValue(byte[] value) {
    long hashValue = MurmurHash3.getHash64AsLong(value);
    if ((hashValue & mask) != 0) {
      return;
    }

    valueSet.add(hashValue);
    if (valueSet.size() > maxSize) {
      mask = mask * 2 + 1;
      List<Long> toDelList = new ArrayList<>();
      valueSet.forEach(v -> {
        if ((v & mask) != 0) {
          toDelList.add(v);
        }
      });
      for (long v : toDelList) {
        valueSet.remove(v);
      }
    }
  }

  /**
   * Merge two FMS together
   *
   * @param fmSketch the other FMS
   */
  public void merge(FlajoletMartinSketch fmSketch) {
  }


  public static FlajoletMartinSketch fromPb(Statspb.FMSketch pbData) {
    if (pbData == null) {
      return null;
    }

    FlajoletMartinSketch fms = new FlajoletMartinSketch(pbData.getSetCount(), pbData.getMask());
    fms.valueSet.addAll(pbData.getSetList());
    return fms;
  }
}
