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

package io.jimdb.sql.optimizer.physical;

import io.jimdb.core.expression.ColumnExpr;

/**
 * Physical property defines the properties needed when finding the best task.
 * TODO add more explanations here
 */
public class PhysicalProperty {
  public static final PhysicalProperty DEFAULT_PROP = new PhysicalProperty(Task.TaskType.PROXY);

  Task.TaskType taskType;

  PhysicalProperty.Entry[] entries;

  public PhysicalProperty(Task.TaskType type, ColumnExpr[] columnExprs) {
    this.taskType = type;
    initEntries(columnExprs);
  }

  public PhysicalProperty(ColumnExpr[] columnExprs) {
    initEntries(columnExprs);
  }

  PhysicalProperty(Task.TaskType type) {
    taskType = type;
  }

  private void initEntries(ColumnExpr[] columnExprs) {
    this.entries = new PhysicalProperty.Entry[columnExprs.length];
    for (int i = 0; i < columnExprs.length; i++) {
      this.entries[i] = new PhysicalProperty.Entry(columnExprs[i]);
    }
  }

  public boolean isEmpty() {
    return entries == null || entries.length == 0;
  }

  public boolean prefixWith(PhysicalProperty childProp) {
    if (this.entries == null || this.entries.length == 0) {
      return true;
    }

    if (childProp.entries == null) {
      return false;
    }

    if (this.entries.length > childProp.entries.length) {
      return false;
    }

    for (int i = 0; i < this.entries.length; i++) {
      if (!entries[i].columnExpr.equals(childProp.entries[i].columnExpr)
              || entries[i].desc != childProp.entries[i].desc) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check that all columns are in the same order
   *
   * @return
   */
  public boolean isOrderConsistent() {
    if (entries == null || entries.length == 0) {
      return true;
    }

    boolean desc = entries[0].desc;
    for (PhysicalProperty.Entry element : entries) {
      if (element.desc != desc) {
        return false;
      }
    }
    return true;
  }

  void setTaskType(Task.TaskType taskType) {
    this.taskType = taskType;
  }

  /**
   * @version V1.0
   */
  public static class Entry {
    public ColumnExpr columnExpr;
    public boolean desc;

    public Entry(ColumnExpr columnExpr) {
      this.columnExpr = columnExpr;
    }
  }
}
