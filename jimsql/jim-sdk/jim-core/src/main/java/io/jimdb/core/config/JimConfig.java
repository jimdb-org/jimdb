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
package io.jimdb.core.config;

import java.io.Closeable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import io.jimdb.common.config.BaseConfig;
import io.jimdb.common.config.NettyClientConfig;
import io.jimdb.common.config.NettyServerConfig;
import io.jimdb.common.config.SystemProperties;
import io.jimdb.common.utils.lang.UncaughtExceptionHandlerImpl;
import io.jimdb.common.utils.lang.NetworkUtil;
import io.jimdb.common.utils.os.OperatingSystemInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.scheduler.ExecutorSchedulerFactory;

/**
 * @version V1.0
 */
@SuppressFBWarnings("HES_EXECUTOR_OVERWRITTEN_WITHOUT_SHUTDOWN")
public final class JimConfig extends BaseConfig implements Closeable {
  /* Number of business threads used for request processing.
   * IF =0 then use num of availableProcessors,
   * IF <0 then use Executor of inbound.
   */
  private static final String EXECUTOR_OUTBOUND_THREADS = "jim.outbound.threads";

  /* Number of business threads used for response processing.
   * IF =0 then use num of availableProcessors,
   * IF <0 then use Executor of outbound.
   */
  private static final String EXECUTOR_INBOUND_THREADS = "jim.inbound.threads";

  private static final String PLUGIN_ROUTERSTORE = "jim.plugin.routerstore";
  private static final String PLUGIN_METASTORE = "jim.plugin.metastore";
  private static final String PLUGIN_SQLENGINE = "jim.plugin.sqlengine";
  private static final String PLUGIN_SQLEXECUTOR = "jim.plugin.sqlexecutor";
  private static final String PLUGIN_STOREENGINE = "jim.plugin.storeengine";
  private static final String PLUGIN_PRIVILEGE = "jim.plugin.privilege";

  private static final String META_MASTER_ADDRESS = "jim.master.address";
  private static final String META_CLUSTER = "jim.meta.cluster";
  private static final String META_LEASE = "jim.meta.lease";
  private static final String META_RETRY = "jim.meta.retry";
  private static final String META_STORE_ADDRESS = "jim.meta.store.address";
  private static final String META_REORG_THREADS = "jim.meta.reorg.threads";
  private static final String META_PRVILEGE_LEASE = "jim.meta.prvilege.lease";
  private static final String META_PRVILEGE_STORE = "jim.meta.prvilege.store";
  private static final String META_PRVILEGE_REPLICA = "jim.meta.prvilege.replica";

  private static final String ROW_ID_STEP = "jim.rowid.step";

  private static final String ENABLE_STATS_PUSH_DOWN = "jim.sql.optimizer.stats.pushdown";
  private static final String ENABLE_BACKGROUND_STATS_COLLECTOR = "jim.sql.optimizer.stats.enable";

  private String serverID;
  private long metaLease;
  private long metaCluster;
  private int reorgThreads;
  private int prvilegeLease;
  private int rowIdStep;
  private int privilegeReplica;
  private int retry;
  private String privilegeStore;
  private String masterAddr;
  private String metaStoreAddress;
  private String metaStoreClass;
  private String sqlEngineClass;
  private String sqlExecutorClass;
  private String storeEngineClass;
  private String privilegeClass;
  private String routerStoreClass;

  private ExecutorService outboundExecutor;
  private ExecutorService inboundExecutor;
  private ExecutorSchedulerFactory schedulerFactory;

  private final NettyServerConfig serverConfig;
  private final NettyClientConfig clientConfig;

  // Sql optimization related options
  private boolean enableStatsPushDown;

  // enable the background thread to collect table stats info periodically
  private boolean enableBackgroundStatsCollector;

  public JimConfig(final Properties props) {
    super(props);
    SystemProperties.init(props);

    this.loadProps();
    this.serverConfig = new NettyServerConfig(this);
    if (this.serverConfig.getWorkExecutor() == null) {
      this.serverConfig.setWorkExecutor(this.outboundExecutor);
    }

    this.clientConfig = new NettyClientConfig(this);
    if (this.clientConfig.getIoLoopGroup() == null) {
      this.clientConfig.setIoLoopGroup(this.serverConfig.getIoLoopGroup());
    }
    if (this.clientConfig.getWorkExecutor() == null) {
      this.clientConfig.setWorkExecutor(this.inboundExecutor);
    }

    String startTime = new SimpleDateFormat("yyyy.MM.dd HH.mm.ss.S").format(new Date());
    this.serverID = String.format("%s_%s_%s", NetworkUtil.getIP(), OperatingSystemInfo.getPid(), startTime);
  }

  private void loadProps() {
    this.masterAddr = getString(META_MASTER_ADDRESS, "");
    this.metaCluster = getLong(META_CLUSTER, 0);
    this.metaLease = getLong(META_LEASE, 5 * 1000);
    this.prvilegeLease = getInt(META_PRVILEGE_LEASE, 10 * 60 * 1000);
    this.rowIdStep = getInt(ROW_ID_STEP, 1000);
    this.reorgThreads = getInt(META_REORG_THREADS, 4);
    this.privilegeReplica = getInt(META_PRVILEGE_REPLICA, 3);
    this.retry = getInt(META_RETRY, 2);
    this.privilegeStore = getString(META_PRVILEGE_STORE, "DISK");
    this.metaStoreAddress = getString(META_STORE_ADDRESS, "");
    this.metaStoreClass = getString(PLUGIN_METASTORE, "io.jimdb.meta.EtcdMetaStore");
    this.sqlEngineClass = getString(PLUGIN_SQLENGINE, "io.jimdb.mysql.MySQLEngine");
    this.sqlExecutorClass = getString(PLUGIN_SQLEXECUTOR, "io.jimdb.sql.JimSQLExecutor");
    this.storeEngineClass = getString(PLUGIN_STOREENGINE, "io.jimdb.engine.ExecutionEngine");
    this.privilegeClass = getString(PLUGIN_PRIVILEGE, "io.jimdb.privilege.SimplePrivilegeManager");
    this.routerStoreClass = getString(PLUGIN_ROUTERSTORE, "io.jimdb.meta.client.MasterClient");
    this.enableStatsPushDown = getBoolean(ENABLE_STATS_PUSH_DOWN, true);
    this.enableBackgroundStatsCollector = getBoolean(ENABLE_BACKGROUND_STATS_COLLECTOR, false);

    int outThreads = this.getInt(EXECUTOR_OUTBOUND_THREADS, -1);
    int inThreads = this.getInt(EXECUTOR_INBOUND_THREADS, -1);
    if (outThreads >= 0) {
      this.outboundExecutor = new ForkJoinPool(outThreads == 0 ? Runtime.getRuntime().availableProcessors() : outThreads,
              ForkJoinPool.defaultForkJoinWorkerThreadFactory, UncaughtExceptionHandlerImpl.getInstance(), true);
    }
    if (inThreads >= 0) {
      this.inboundExecutor = new ForkJoinPool(inThreads == 0 ? Runtime.getRuntime().availableProcessors() : inThreads,
              ForkJoinPool.defaultForkJoinWorkerThreadFactory, UncaughtExceptionHandlerImpl.getInstance(), true);
      this.outboundExecutor = this.outboundExecutor == null ? this.inboundExecutor : this.outboundExecutor;
    } else if (this.outboundExecutor != null) {
      this.inboundExecutor = this.outboundExecutor;
    } else {
      this.inboundExecutor = this.outboundExecutor = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
              ForkJoinPool.defaultForkJoinWorkerThreadFactory, UncaughtExceptionHandlerImpl.getInstance(), true);
    }
  }

  public String getServerID() {
    return serverID;
  }

  public String getMetaStoreAddress() {
    return metaStoreAddress;
  }

  public String getMasterAddr() {
    return masterAddr;
  }

  public long getMetaCluster() {
    return metaCluster;
  }

  public int getReorgThreads() {
    return reorgThreads;
  }

  public long getMetaLease() {
    return metaLease;
  }

  public int getRowIdStep() {
    return rowIdStep;
  }

  public int getPrvilegeLease() {
    return prvilegeLease;
  }

  public int getPrivilegeReplica() {
    return privilegeReplica;
  }

  public int getRetry() {
    return retry;
  }

  public String getPrivilegeStore() {
    return privilegeStore;
  }

  public ExecutorService getOutboundExecutor() {
    return outboundExecutor;
  }

  public ExecutorService getInboundExecutor() {
    return inboundExecutor;
  }

  public NettyServerConfig getServerConfig() {
    return serverConfig;
  }

  public NettyClientConfig getClientConfig() {
    return clientConfig;
  }

  public String getMetaStoreClass() {
    return metaStoreClass;
  }

  public String getSqlEngineClass() {
    return sqlEngineClass;
  }

  public String getSqlExecutorClass() {
    return sqlExecutorClass;
  }

  public String getStoreEngineClass() {
    return storeEngineClass;
  }

  public String getPrivilegeClass() {
    return privilegeClass;
  }

  public String getRouterStoreClass() {
    return routerStoreClass;
  }

  public ExecutorSchedulerFactory getSchedulerFactory() {
    synchronized (this) {
      if (schedulerFactory == null && getOutboundExecutor() != null) {
        schedulerFactory = new ExecutorSchedulerFactory(getOutboundExecutor(), Math.max(8, Runtime.getRuntime().availableProcessors()));
      }
      return schedulerFactory;
    }
  }

  public boolean isEnableStatsPushDown() {
    return this.enableStatsPushDown;
  }

  public boolean isEnableBackgroundStatsCollector() {
    return enableBackgroundStatsCollector;
  }

  // test only
  public void setEnableBackgroundStatsCollector(boolean enableBackgroundStatsCollector) {
    this.enableBackgroundStatsCollector = enableBackgroundStatsCollector;
  }

  @Override
  public void close() {
    if (serverConfig != null) {
      serverConfig.close();
    }
    if (clientConfig != null) {
      clientConfig.close();
    }
    if (outboundExecutor != null) {
      outboundExecutor.shutdown();
    }
    if (inboundExecutor != null) {
      inboundExecutor.shutdown();
    }
    if (schedulerFactory != null) {
      schedulerFactory.close();
    }
  }
}
