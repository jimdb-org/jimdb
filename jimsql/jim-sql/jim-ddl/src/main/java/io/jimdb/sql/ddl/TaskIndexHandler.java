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
package io.jimdb.sql.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.lang.ByteUtil;
import io.jimdb.core.ExecutorHelper;
import io.jimdb.core.codec.Codec;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.meta.RangeInfo;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.MetaStore.TaskType;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Ddlpb;
import io.jimdb.pb.Ddlpb.AddIndexInfo;
import io.jimdb.pb.Ddlpb.IndexReorg;
import io.jimdb.pb.Ddlpb.Task;
import io.jimdb.pb.Ddlpb.TaskState;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.IndexInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.pb.Mspb;

import com.google.protobuf.ByteString;
import com.google.protobuf.NettyByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "OCP_OVERLY_CONCRETE_PARAMETER" })
final class TaskIndexHandler {
  private static final char COMMA = ',';
  private static final char DOT = '.';

  private TaskIndexHandler() {
  }

  static AddIndexInfo addIndex(MetaStore metaStore, RouterStore routerStore, Engine storeEngine,
                               long taskId, TableInfo tableInfo, AddIndexInfo addIndexInfo) {
    int pos = 0;
    IndexInfo index = null;
    TableInfo.Builder tableBuilder = tableInfo.toBuilder();
    for (IndexInfo indexInfo : tableBuilder.getIndicesList()) {
      if (indexInfo.getId() == addIndexInfo.getId()) {
        index = indexInfo;
        break;
      }
      ++pos;
    }
    if (index == null) {
      index = buildIndex(addIndexInfo, tableBuilder);
      pos = tableBuilder.getIndicesCount() - 1;
      TableInfo oldTable = tableInfo;
      tableInfo = tableBuilder.build();
      if (!metaStore.storeTable(oldTable, tableInfo)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
      }
    }

    switch (index.getState()) {
      case Absent:
        createIndexRange(routerStore, tableBuilder.build(), index);
        index = index.toBuilder()
                .setState(MetaState.DeleteOnly)
                .build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addIndexInfo.toBuilder().setState(MetaState.DeleteOnly).build();

      case DeleteOnly:
        index = index.toBuilder()
                .setState(MetaState.WriteOnly)
                .build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addIndexInfo.toBuilder().setState(MetaState.WriteOnly).build();

      case WriteOnly:
        index = index.toBuilder()
                .setState(MetaState.WriteReorg)
                .build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return addIndexInfo.toBuilder().setState(MetaState.WriteReorg).build();

      case WriteReorg:
        doReorg(taskId, tableInfo, index, metaStore, storeEngine);
        index = index.toBuilder()
                .setState(MetaState.Public)
                .build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        metaStore.removeTask(TaskType.INDEXTASK, taskId);
        return addIndexInfo.toBuilder().setState(MetaState.Public).build();

      case Public:
        metaStore.removeTask(TaskType.INDEXTASK, taskId);
        return addIndexInfo.toBuilder().setState(MetaState.Public).build();

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, "invalid table state: " + index.getState().name());
    }
  }

  static IndexInfo dropIndex(MetaStore metaStore, RouterStore routerStore, TableInfo tableInfo, IndexInfo dropIndex) {
    int pos = 0;
    IndexInfo index = null;
    TableInfo.Builder tableBuilder = tableInfo.toBuilder();
    for (IndexInfo indexInfo : tableBuilder.getIndicesList()) {
      if (indexInfo.getName().equalsIgnoreCase(dropIndex.getName())) {
        if (dropIndex.getId() == 0 || dropIndex.getId() == indexInfo.getId()) {
          index = indexInfo;
          break;
        }
      }
      ++pos;
    }

    if (index == null) {
      if (dropIndex.getId() == 0) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_KEY_DOES_NOT_EXITS, dropIndex.getName(), tableInfo.getName());
      }
      return dropIndex.toBuilder().setState(MetaState.Absent).build();
    }
    if (index.getPrimary()) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DROP_INDEX_FK, dropIndex.getName());
    }

    switch (index.getState()) {
      case Public:
        index = index.toBuilder().setState(MetaState.WriteOnly).build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.WriteOnly)
                .build();

      case WriteReorg:
        index = index.toBuilder().setState(MetaState.WriteOnly).build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.WriteOnly)
                .build();

      case WriteOnly:
        index = index.toBuilder().setState(MetaState.DeleteOnly).build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.DeleteOnly)
                .build();

      case DeleteOnly:
        index = index.toBuilder().setState(MetaState.DeleteReorg).build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.DeleteReorg)
                .build();

      case DeleteReorg:
        index = index.toBuilder().setState(MetaState.Absent).build();
        tableBuilder.setIndices(pos, index);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.DeleteReorg)
                .build();

      case Absent:
        deleteIndexRange(routerStore, tableInfo, index);
        tableBuilder.removeIndices(pos);
        if (!metaStore.storeTable(tableInfo, tableBuilder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return dropIndex.toBuilder()
                .setId(index.getId())
                .setState(MetaState.Absent)
                .build();

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, "invalid table state: " + index.getState().name());
    }
  }

  static MetaState renameIndex(MetaStore metaStore, TableInfo tableInfo, String from, String to) {
    int pos = 0;
    IndexInfo indexInfo = null;
    List<IndexInfo> indexInfos = tableInfo.getIndicesList();
    for (int i = 0; i < indexInfos.size(); i++) {
      IndexInfo index = indexInfos.get(i);
      if (index.getName().equalsIgnoreCase(to)) {
        throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_DUP_KEYNAME, to);
      }
      if (index.getState() == MetaState.Public && index.getName().equalsIgnoreCase(from)) {
        pos = i;
        indexInfo = index;
      }
    }

    if (indexInfo == null) {
      throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_KEY_DOES_NOT_EXITS, from, tableInfo.getName());
    }

    indexInfo = indexInfo.toBuilder().setName(to).build();
    TableInfo.Builder builder = tableInfo.toBuilder().setIndices(pos, indexInfo);
    if (!metaStore.storeTable(tableInfo, builder.build())) {
      throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", builder.getId()));
    }
    return MetaState.Public;
  }

  private static IndexInfo buildIndex(AddIndexInfo addIndexInfo, TableInfo.Builder tableBuilder) {
    IndexInfo.Builder indexBuilder = IndexInfo.newBuilder();
    indexBuilder.setId(addIndexInfo.getId())
            .setName(addIndexInfo.getName())
            .setUnique(addIndexInfo.getUnique())
            .setPrimary(addIndexInfo.getPrimary())
            .setTableId(tableBuilder.getId())
            .setCreateTime(addIndexInfo.getCreateTime())
            .setComment(addIndexInfo.getComment())
            .setType(addIndexInfo.getType());

    List<ColumnInfo> retainCols = new ArrayList<>(tableBuilder.getColumnsCount());
    Map<String, ColumnInfo> tableCols = new HashMap<>(tableBuilder.getColumnsCount());
    for (ColumnInfo column : tableBuilder.getColumnsList()) {
      if (column.getState() != MetaState.Public) {
        retainCols.add(column);
        continue;
      }
      tableCols.put(column.getName().toLowerCase(), column);
    }
    List<IndexInfo> indexs = tableBuilder.getIndicesList();
    IndexInfo indexInfo = DDLUtils.buildIndexInfo(indexBuilder.build(), addIndexInfo.getColumnsList(), tableCols, indexs);

    retainCols.addAll(tableCols.values());
    Collections.sort(retainCols, (c1, c2) -> {
      if (c1.getOffset() == c2.getOffset()) {
        return 0;
      }
      return c1.getOffset() > c2.getOffset() ? 1 : -1;
    });
    tableBuilder.clearColumns()
            .addAllColumns(retainCols)
            .addIndices(indexInfo);
    return indexInfo;
  }

  private static void createIndexRange(RouterStore routerStore, TableInfo tableInfo, IndexInfo indexInfo) {
    Map<Integer, ColumnInfo> colMap = new HashMap<>(tableInfo.getColumnsCount());
    for (ColumnInfo columnInfo : tableInfo.getColumnsList()) {
      colMap.put(columnInfo.getId(), columnInfo);
    }
    List<Basepb.Range> indexRanges = DDLUtils.buildIndexRange(tableInfo, colMap, Collections.singletonList(indexInfo));
    Mspb.CreateRangesRequest.Builder requestBuilder = Mspb.CreateRangesRequest.newBuilder()
            .setDbId(tableInfo.getDbId())
            .setTableId(tableInfo.getId())
            .setReplicas(tableInfo.getReplicas())
            .addAllRanges(indexRanges);
    routerStore.createRange(requestBuilder.build());
  }

  private static void deleteIndexRange(RouterStore routerStore, TableInfo tableInfo, IndexInfo indexInfo) {
    Mspb.DeleteRangesRequest.Builder builder = Mspb.DeleteRangesRequest.newBuilder()
            .setDbId(tableInfo.getDbId())
            .setTableId(tableInfo.getId())
            .setIndexId(indexInfo.getId())
            .setRangeType(Basepb.RangeType.RNG_Index);

    routerStore.deleteRange(builder.build());
  }

  private static void doReorg(long taskId, TableInfo tableInfo, IndexInfo indexInfo, MetaStore metaStore, Engine storeEngine) {
    Task task = metaStore.getTask(TaskType.INDEXTASK, Task.newBuilder().setId(taskId).build());
    if (task == null) {
      if (!createReorgTask(taskId, tableInfo, indexInfo, metaStore, storeEngine)) {
        return;
      }
    }

    waitReorgComplete(taskId, tableInfo.getId(), metaStore);
  }

  private static void waitReorgComplete(long taskId, int tableId, MetaStore metaStore) {
    while (true) {
      Task tableTask = Task.newBuilder()
              .setId(taskId)
              .setTableId(tableId)
              .setOp(Ddlpb.OpType.AddIndex)
              .build();
      if (!metaStore.tryLock(TaskType.SCHEMATASK, tableTask)) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, "lost the ddl task worker lock");
      }

      Task taskStatus = metaStore.getTask(TaskType.INDEXTASK, Task.newBuilder().setId(taskId).build());
      if (taskStatus == null) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableId));
      }
      if (taskStatus.getState() != TaskState.Running) {
        throw new DDLException(DDLException.ErrorType.ROLLACK, ErrorCode.valueOf(taskStatus.getErrorCode()), false, taskStatus.getError());
      }

      List<Task> indexTasks = metaStore.getTasks(TaskType.INDEXTASK, Task.newBuilder().setId(taskId).build());
      if (indexTasks == null || indexTasks.isEmpty()) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableId));
      }

      boolean isComplete = true;
      for (Task task : indexTasks) {
        if (task.getState() != TaskState.Success) {
          isComplete = false;
          break;
        }
      }
      tableTask = null;
      taskStatus = null;
      indexTasks = null;

      if (isComplete) {
        return;
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException ex) {
        throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, ex);
      }
    }
  }

  private static boolean createReorgTask(long taskId, TableInfo tableInfo, IndexInfo indexInfo, MetaStore metaStore, Engine storeEngine) {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException ex) {
      throw DBException.get(ErrorModule.DDL, ErrorCode.ER_INTERNAL_ERROR, ex);
    }

    Index index = null;
    Table table = MetaData.Holder.get().getTable(tableInfo.getDbId(), tableInfo.getId());
    if (table != null) {
      final String indexName = indexInfo.getName();
      for (Index idx : table.getWritableIndices()) {
        if (idx.getName().equalsIgnoreCase(indexName)) {
          index = idx;
          break;
        }
      }
    }
    if (index == null) {
      throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
    }

    StringBuilder sb = new StringBuilder();
    for (Column column : table.getPrimary()) {
      sb.append(column.getName()).append(COMMA);
    }
    String keys = sb.substring(0, sb.length() - 1);
    sb = new StringBuilder("select ");
    sb.append(keys)
            .append(" from ")
            .append(table.getCatalog().getName())
            .append(DOT)
            .append(table.getName())
            .append(" order by ")
            .append(keys)
            .append(" desc limit 1");
    List<ExecResult> results = ExecutorHelper.execute(PluginFactory.getSqlExecutor(), PluginFactory.getSqlEngine(), storeEngine, sb.toString());
    if (results == null || results.isEmpty() || results.get(0).size() == 0) {
      return false;
    }

    Value[] values = new Value[table.getPrimary().length];
    for (int i = 0; i < table.getPrimary().length; i++) {
      values[i] = results.get(0).getRow(0).get(i);
    }
    ByteString maxKey = Codec.encodeKey(table.getId(), table.getPrimary(), values);
    maxKey = Codec.nextKey(maxKey);

    Set<RangeInfo> ranges = storeEngine.getRanges(table);
    List<Task> indexTasks = new ArrayList<>(ranges.size());
    int i = 0;
    for (RangeInfo range : ranges) {
      ByteString startKey = NettyByteString.wrap(range.getStartKey());
      ByteString endKey = NettyByteString.wrap(range.getEndKey());
      if (ByteUtil.compare(startKey, maxKey) >= 0) {
        continue;
      }
      if (ByteUtil.compare(endKey, maxKey) > 0) {
        endKey = maxKey;
      }

      IndexReorg reorg = IndexReorg.newBuilder()
              .setIndexId(indexInfo.getId())
              .setOffset(++i)
              .setStartKey(startKey)
              .setEndKey(endKey)
              .setLastKey(startKey)
              .build();

      Task indexTask = Task.newBuilder()
              .setId(taskId)
              .setDbId(tableInfo.getDbId())
              .setTableId(tableInfo.getId())
              .setData(reorg.toByteString())
              .setState(TaskState.Running)
              .build();

      indexTasks.add(indexTask);
    }

    if (indexTasks.isEmpty()) {
      return false;
    }

    metaStore.storeTask(TaskType.INDEXTASK, null, indexTasks.toArray(new Task[0]));
    return true;
  }
}
