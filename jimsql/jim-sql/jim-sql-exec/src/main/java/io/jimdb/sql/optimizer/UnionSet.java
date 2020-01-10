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
package io.jimdb.sql.optimizer;

/**
 * Class to manage union set for column constant propagation.
 */
public class UnionSet {
  int[] unionSet;

  public UnionSet(int size) {
    unionSet = new int[size];
    for (int i = 0; i < size; i++) {
      unionSet[i] = i;
    }
  }

  public int findSetRoot(int item) {
    while (item != unionSet[item]) {
      item = unionSet[item];
    }
    return item;
  }

  public int mergeSet(int s1, int s2) {
    int r1 = findSetRoot(s1);
    int r2 = findSetRoot(s2);
    unionSet[r1] = r2;
    return r2;
  }
}
