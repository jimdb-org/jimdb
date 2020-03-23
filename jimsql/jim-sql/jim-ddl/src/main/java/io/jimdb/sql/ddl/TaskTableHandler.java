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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.jimdb.common.exception.ErrorCode;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.plugin.RouterStore;
import io.jimdb.core.values.ValueChecker;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.ColumnInfo;
import io.jimdb.pb.Metapb.MetaState;
import io.jimdb.pb.Metapb.TableInfo;
import io.jimdb.pb.Mspb;
import io.jimdb.pb.Mspb.DeleteRangesRequest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Responsible for handling table task.
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "OCP_OVERLY_CONCRETE_PARAMETER" })
final class TaskTableHandler {
  private TaskTableHandler() {
  }

  static TableInfo createTable(MetaStore metaStore, RouterStore routerStore, int dbId, TableInfo tableInfo) {
    String tableName = DDLUtils.splitTableName(tableInfo.getName()).getT2();
    List<TableInfo> tables = metaStore.getTables(dbId);
    if (tables != null) {
      for (TableInfo existTable : tables) {
        if (existTable.getId() == tableInfo.getId()) {
          tableInfo = existTable;
          break;
        }
        if (existTable.getState() == MetaState.Public && existTable.getName().equalsIgnoreCase(tableName)) {
          throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_TABLE_EXISTS_ERROR, tableName);
        }
      }
    }

    tableInfo = tableInfo.toBuilder().setDbId(dbId).build();
    Metapb.MetaState state = tableInfo.getState();
    switch (state) {
      case Absent:
        int replicaNum = tableInfo.getReplicas();
        if (replicaNum == 0) {
          replicaNum = DDLUtils.DEFAULT_REPLICA_NUM;
        }

        //store auto_increment meta
        long autoInitId = tableInfo.getAutoInitId();
        ColumnInfo autoIncrColumn = getAutoColumn(tableInfo);
        if (autoIncrColumn != null) {
          Metapb.AutoIdInfo autoIdInfo = buildAutoInfo(autoInitId, autoIncrColumn.getSqlType());
          Metapb.AutoIdInfo existValue = metaStore.storeAutoIdInfo(dbId, tableInfo.getId(), autoIdInfo, null);
          if (existValue != null) {
            throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
          }
        }

        //create range on master
        createRange(routerStore, tableInfo, replicaNum);
        //store table meta
        tableInfo = tableInfo.toBuilder()
                .setDbId(dbId)
                .setName(tableName)
                .setAutoInitId(autoInitId)
                .setReplicas(replicaNum)
                .setState(MetaState.Public)
                .build();
        if (!metaStore.storeTable(null, tableInfo)) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
        }

        return tableInfo;

      case Public:
        return tableInfo;

      default:
        throw new DDLException(DDLException.ErrorType.FAILED, String.format("invalid tableInfo state %s", state.name()));
    }
  }

  static MetaState dropTable(MetaStore metaStore, RouterStore routerStore, int dbId, int tableId) {
    TableInfo.Builder builder = null;
    TableInfo oldTable = null;
    MetaState state = MetaState.Absent;
    oldTable = metaStore.getTable(dbId, tableId);
    if (oldTable != null) {
      state = oldTable.getState();
      builder = oldTable.toBuilder();
    }

    switch (state) {
      case Public:
        builder.setState(MetaState.WriteOnly);
        if (!metaStore.storeTable(oldTable, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.WriteOnly;
      case WriteOnly:
        builder.setState(MetaState.DeleteOnly);
        if (!metaStore.storeTable(oldTable, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.DeleteOnly;
      case DeleteOnly:
        builder.setState(MetaState.Absent);
        if (!metaStore.storeTable(oldTable, builder.build())) {
          throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", builder.getId()));
        }
        return MetaState.DeleteOnly;
      default:
        doDropTable(metaStore, routerStore, dbId, tableId);
        return MetaState.Absent;
    }
  }

  static MetaState renameTable(MetaStore metaStore, TableInfo tableInfo, String name) {
    if (tableInfo.getName().equalsIgnoreCase(name)) {
      return MetaState.Public;
    }

    List<TableInfo> tables = metaStore.getTables(tableInfo.getDbId());
    if (tables != null) {
      for (TableInfo existTable : tables) {
        if (existTable.getName().equalsIgnoreCase(name)) {
          throw new DDLException(DDLException.ErrorType.FAILED, ErrorCode.ER_TABLE_EXISTS_ERROR, name);
        }
      }
    }

    TableInfo.Builder builder = tableInfo.toBuilder().setName(name);
    if (!metaStore.storeTable(tableInfo, builder.build())) {
      throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", builder.getId()));
    }
    return MetaState.Public;
  }

  static MetaState alterAutoInitId(MetaStore metaStore, TableInfo tableInfo, long alterId) {
    ColumnInfo autoIncrColumn = getAutoColumn(tableInfo);
    if (autoIncrColumn == null) {
      return MetaState.Public;
    }

    boolean neecUpdate = true;
    Metapb.AutoIdInfo oldAutoIdInfo = metaStore.getAutoIdInfo(tableInfo.getDbId(), tableInfo.getId());
    if (oldAutoIdInfo != null) {
      BigInteger oldInitId = new BigInteger(Long.toUnsignedString(oldAutoIdInfo.getInitId()));
      long step = oldAutoIdInfo.getStep();
      List<Long> startList = oldAutoIdInfo.getStartList();
      for (int i = 0; i < startList.size(); i++) {
        BigInteger needStart = oldInitId.add(new BigInteger(Long.toUnsignedString(step * i)));
        BigInteger allocStart = new BigInteger(Long.toUnsignedString(startList.get(i)));
        if (needStart.compareTo(allocStart) != 0) {
          neecUpdate = false;
          BigInteger alterInitId = new BigInteger(Long.toUnsignedString(alterId));
          if (alterInitId.compareTo(oldInitId) > 0) {
            throw new DDLException(DDLException.ErrorType.FAILED, String.format("invalid auto_increment_id %d, old auto_increment_id is %s)",
                    alterId, oldInitId.toString()));
          }
          break;
        }
      }
    }

    if (neecUpdate) {
      Metapb.AutoIdInfo autoIdInfo = buildAutoInfo(alterId, autoIncrColumn.getSqlType());
      Metapb.AutoIdInfo existValue = metaStore.storeAutoIdInfo(tableInfo.getDbId(), tableInfo.getId(), autoIdInfo, oldAutoIdInfo);
      if (existValue != null) {
        throw new DDLException(DDLException.ErrorType.CONCURRENT, String.format("Table[%d] occur concurrent process error", tableInfo.getId()));
      }
    }
    return MetaState.Public;
  }

  private static ColumnInfo getAutoColumn(Metapb.TableInfo tableInfo) {
    Optional<ColumnInfo> any = tableInfo.getColumnsList().stream()
            .filter(columnInfo -> columnInfo.getPrimary() && columnInfo.getAutoIncr() && columnInfo.getSqlType().getUnsigned()).findAny();
    if (any.isPresent()) {
      return any.get();
    }
    return null;
  }

  private static Metapb.AutoIdInfo buildAutoInfo(long autoInitId, Metapb.SQLType type) {
    List<Long> scopeStartList = new ArrayList<>(DDLUtils.DEFAULT_SEQ_REGION);
    if (autoInitId == 0) {
      autoInitId = 1;
    }

    BigInteger autoInitIdValue = new BigInteger(Long.toUnsignedString(autoInitId));
    BigInteger maxValue = ValueChecker.getUpperBound(type.getUnsigned(), type.getType());
    //calculate auto_increment scope
    long sequence = maxValue.subtract(autoInitIdValue).add(BigInteger.valueOf(1))
            .divide(BigInteger.valueOf(DDLUtils.DEFAULT_SEQ_REGION)).longValue();
    for (int i = 0; i < DDLUtils.DEFAULT_SEQ_REGION; i++) {
      BigInteger bigInteger = new BigInteger(Long.toUnsignedString(sequence * i)).add(autoInitIdValue);
      scopeStartList.add(bigInteger.longValue());
    }
    return Metapb.AutoIdInfo.newBuilder()
            .setInitId(autoInitId)
            .setStep(sequence)
            .addAllStart(scopeStartList)
            .build();
  }

  private static void createRange(RouterStore routerStore, TableInfo tableInfo, int replicaNum) {
    //build range
    List<Basepb.Range> rangeList = buildRangesForCreateTable(tableInfo);
    //call create range interface
    Mspb.CreateRangesRequest.Builder requestBuilder = Mspb.CreateRangesRequest.newBuilder()
            .setDbId(tableInfo.getDbId())
            .setTableId(tableInfo.getId())
            .setReplicas(replicaNum)
            .addAllRanges(rangeList);

    routerStore.createRange(requestBuilder.build());
  }

  private static List<Basepb.Range> buildRangesForCreateTable(TableInfo tableInfo) {
    Map<Integer, ColumnInfo> colMap = new HashMap<>(tableInfo.getColumnsCount());
    for (ColumnInfo columnInfo : tableInfo.getColumnsList()) {
      colMap.put(columnInfo.getId(), columnInfo);
    }

    //create range
    List<Basepb.Range> rowRanges = DDLUtils.buildTableRange(tableInfo, colMap);
    List<Basepb.Range> indexRanges = DDLUtils.buildIndexRange(tableInfo, colMap, tableInfo.getIndicesList());
    if (indexRanges == null || indexRanges.isEmpty()) {
      return rowRanges;
    }
    rowRanges.addAll(indexRanges);
    return rowRanges;
  }

  private static void doDropTable(MetaStore metaStore, RouterStore routerStore, int dbId, int tableId) {
    DeleteRangesRequest.Builder builder = DeleteRangesRequest.newBuilder();
    builder.setDbId(dbId).setTableId(tableId);
    routerStore.deleteRange(builder.build());
    metaStore.removeTable(dbId, tableId);
  }
}
