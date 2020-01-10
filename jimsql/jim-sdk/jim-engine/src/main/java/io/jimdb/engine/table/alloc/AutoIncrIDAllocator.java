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
package io.jimdb.engine.table.alloc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.pb.Metapb;
import io.jimdb.core.plugin.MetaStore;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.ValueConvertor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @version V1.0
 */
public class AutoIncrIDAllocator implements IDAllocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AutoIncrIDAllocator.class);

  private static final UnsignedLongValue INTERVAL = UnsignedLongValue.getInstance(new BigInteger("1"));

  private int step;

  private MetaStore metaStore;

  //dbId#tableId
  private ConcurrentMap<String, TableIDAllocator> tableIDAllocatorMap = new ConcurrentHashMap<>();

  public AutoIncrIDAllocator(MetaStore metaStore, int rowIdStepSize) {
    this.metaStore = metaStore;
    this.step = rowIdStepSize;
  }

  public List<UnsignedLongValue> alloc(int dbId, int tableId, int size) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("alloc id: dbId {}, tableId{}, size{}.", dbId, tableId, size);
    }
    return getTableIDAllocator(dbId, tableId).alloc(size);
  }

  private TableIDAllocator getTableIDAllocator(int dbId, int tableId) {
    String key = getKey(dbId, tableId);
    TableIDAllocator allocator = tableIDAllocatorMap.get(key);
    if (allocator == null) {
      final TableIDAllocator temp = new TableIDAllocator(this.metaStore, dbId, tableId, this.step);
      allocator = tableIDAllocatorMap.putIfAbsent(key, temp);
      if (allocator == null) {
        return temp;
      }
    }
    return allocator;
  }

  public void removeIDAlloctor(int dbId, int tableId) {
    tableIDAllocatorMap.remove(getKey(dbId, tableId));
  }

  private String getKey(int dbId, int tableId) {
    return String.format("%s#%s", dbId, tableId);
  }

  /**
   * @version V1.0
   */
  public static class TableIDAllocator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableIDAllocator.class);

    private int dbId;
    private int tableId;
    private int step;
    private MetaStore metaStore;
    private static final int RETRY_NUM = 2;

    private ConcurrentLinkedQueue<Tuple2<BigInteger, BigInteger>> idPairQueue = new ConcurrentLinkedQueue<>();

    public TableIDAllocator(MetaStore metaStore, int dbId, int tableId, int step) {
      this.metaStore = metaStore;
      this.dbId = dbId;
      this.tableId = tableId;
      this.step = step;
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private List<Tuple2<BigInteger, BigInteger>> getAutoIncIds(int step) {
      int retry = RETRY_NUM;
      Metapb.AutoIdInfo autoIdInfo;
      while (retry-- > 0) {
        try {
          autoIdInfo = this.metaStore.getAutoIdInfo(this.dbId, this.tableId);
          if (autoIdInfo == null || autoIdInfo.getStartList().isEmpty()) {
            return null;
          }

          List<Long> startList = autoIdInfo.getStartList();
          List<Long> startListAfterAlloc = new ArrayList<>(startList.size());
          int stepNum = startList.size();
          int avg = step / stepNum;
          List<Tuple2<BigInteger, BigInteger>> allocIds = new ArrayList<>(stepNum);
          for (int i = 0; i < stepNum; i++) {
            BigInteger start = new BigInteger(Long.toUnsignedString(startList.get(i)));
            BigInteger alloc;
            if (i != 0) {
              alloc = start.add(BigInteger.valueOf(avg));
            } else {
              alloc = start.add(BigInteger.valueOf(step - (avg * (stepNum - 1))));
            }
            allocIds.add(Tuples.of(start, alloc));
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("alloc auto_increment id:[{}, {})", start, alloc);
            }
            startListAfterAlloc.add(alloc.longValue());
          }
          Metapb.AutoIdInfo afterAlloc = Metapb.AutoIdInfo.newBuilder().setInitId(autoIdInfo.getInitId())
                  .setStep(autoIdInfo.getStep())
                  .addAllStart(startListAfterAlloc).build();
          autoIdInfo = this.metaStore.storeAutoIdInfo(dbId, tableId, afterAlloc, autoIdInfo);
          if (autoIdInfo == null) {
            return allocIds;
          }
        } catch (Throwable e) {
          if (retry == 0) {
            throw e;
          }
        }
      }
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_META_GET_AUTO_INCR);
    }

    private void syncRowIDScope(int size) {
      size = Math.max(size, step);
      List<Tuple2<BigInteger, BigInteger>> allocIds = this.getAutoIncIds(size);
      if (allocIds == null || allocIds.isEmpty()) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("sync auto increment from remote is null, tableId:{}", tableId);
        }
        throw DBException.get(ErrorModule.META, ErrorCode.ER_META_GET_AUTO_INCR);
      }
      allocIds.stream().forEach(ids -> idPairQueue.offer(ids));
    }

    public List<UnsignedLongValue> alloc(int size) {
      List<UnsignedLongValue> values = allocFromQueue(size);
      if (values.size() != size) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_META_GET_AUTO_INCR);
      }
      return values;
    }

    @SuppressFBWarnings("PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS")
    private List<UnsignedLongValue> allocFromQueue(int size) {
      List<Tuple2<BigInteger, BigInteger>> unsatisfiedSizeList = new ArrayList<>();
      BigInteger sizeValue = BigInteger.valueOf(size);
      try {
        while (true) {
          Tuple2<BigInteger, BigInteger> idPair;
          if ((idPair = idPairQueue.poll()) == null) {
            synchronized (this) {
              if ((idPair = idPairQueue.poll()) == null) {
                //must sync get, for handing metadata error reported by master
                this.syncRowIDScope(size);
                idPair = idPairQueue.poll();
              }
            }
          }
          if (idPair == null) {
            return null;
          }
          BigInteger start = idPair.getT1();
          BigInteger end = idPair.getT2();
          BigInteger scope = end.subtract(start);
          if (scope.compareTo(sizeValue) < 0) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("alloc auto_increment id size less not need:[{}, {}), scope:{}, size:{}", start, end, scope, sizeValue);
            }
            unsatisfiedSizeList.add(idPair);
            continue;
          }
          BigInteger newEnd = start.add(sizeValue);
          List<UnsignedLongValue> values = convertIdPairs(start, newEnd, size);
          if (scope.compareTo(sizeValue) > 0) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("alloc auto_increment id[{}, {}), use size: {}, newEnd:{}.", start, end, sizeValue, newEnd);
            }
            idPairQueue.offer(Tuples.of(newEnd, end));
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("alloc auto_increment id:[{}, {}) after use.", newEnd, end);
            }
          }
          return values;
        }
      } finally {
        if (!unsatisfiedSizeList.isEmpty()) {
          unsatisfiedSizeList.forEach(idPair -> idPairQueue.offer(idPair));
        }
      }
    }

    private List<UnsignedLongValue> convertIdPairs(BigInteger start, BigInteger end, int size) {
      List<UnsignedLongValue> rangeValues = new ArrayList<>(size);
      UnsignedLongValue startKey = ValueConvertor.convertToUnsignedLong(null, UnsignedLongValue.getInstance(start), null);
      UnsignedLongValue endKey = ValueConvertor.convertToUnsignedLong(null, UnsignedLongValue.getInstance(end), null);
      while (startKey.compareToSafe(endKey) < 0) {
        rangeValues.add(startKey);
        startKey = (UnsignedLongValue) startKey.plus(null, INTERVAL);
      }
      return rangeValues;
    }
  }
}
