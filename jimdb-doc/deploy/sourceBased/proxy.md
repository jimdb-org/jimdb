# Proxy部署

## 目录结构

### jim-server目录下

proxy目录结构

```
├── bin
│   ├── jim.pid
│   ├── nohup.out
│   ├── start.sh
│   └── stop.sh
├── conf
│   ├── jim.properties
│   ├── log4j2.component.properties
│   └── log4j2.xml
└── lib
    ├── animal-sniffer-annotations-1.14.jar
    ├── commons-codec-1.12.jar
    ├── commons-collections-3.2.jar
    ├── commons-lang3-3.8.1.jar
    ├── commons-logging-1.2.jar
    ├── concurrentlinkedhashmap-lru-1.4.2.jar
    ├── disruptor-3.4.2.jar
    ├── druid-1.1.20.jar
    ├── error_prone_annotations-2.0.18.jar
    ├── fastjson-1.2.58.jar
    ├── guava-23.0.jar
    ├── httpclient-4.5.2.jar
    ├── httpcore-4.4.4.jar
    ├── j2objc-annotations-1.1.jar
    ├── jim-common-1.0.0-SNAPSHOT.jar
    ├── jim-core-1.0.0-SNAPSHOT.jar
    ├── jim-engine-1.0.0-SNAPSHOT.jar
    ├── jim-meta-core-1.0.0-SNAPSHOT.jar
    ├── jim-meta-proto-1.0.0-SNAPSHOT.jar
    ├── jim-meta-service-1.0.0-SNAPSHOT.jar
    ├── jim-mysql-model-1.0.0-SNAPSHOT.jar
    ├── jim-mysql-protocol-1.0.0-SNAPSHOT.jar
    ├── jim-privilege-1.0.0-SNAPSHOT.jar
    ├── jim-proto-1.0.0-SNAPSHOT.jar
    ├── jim-rpc-1.0.0-SNAPSHOT.jar
    ├── jim-server-1.0.0-SNAPSHOT.jar
    ├── jim-sql-exec-1.0.0-SNAPSHOT.jar
    ├── jsr305-3.0.2.jar
    ├── log4j-api-2.11.2.jar
    ├── log4j-core-2.11.2.jar
    ├── log4j-slf4j-impl-2.11.2.jar
    ├── netty-all-4.1.39.Final.jar
    ├── reactive-streams-1.0.3.jar
    ├── reactor-core-3.3.0.RELEASE.jar
    ├── slf4j-api-1.7.26.jar
    └── spotbugs-annotations-4.0.0-beta1.jar
```

## conf配置文件 jim.properties

### proxy配置文件

```
opts.memory=-Xms8G -Xmx8G -Xmn3G -XX:SurvivorRatio=8 -XX:MaxDirectMemorySize=4G -XX:MetaspaceSize=64M -XX:MaxMetaspaceSize=512M -Xss256K -server -XX:+TieredCompilation -XX:CICompilerCount=3 -XX:InitialCodeCacheSize=64m -XX:ReservedCodeCacheSize=2048m -XX:CompileThreshold=1000 -XX:FreqInlineSize=2048 -XX:MaxInlineSize=512 -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:CMSMaxAbortablePrecleanTime=100 -XX:+PrintGCDetails -Xloggc:/export/Logs/jimsql/gc.log -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses -XX:+PrintGCTimeStamps
 
#JIM
jim.outbound.threads=0
jim.inbound.threads=0
jim.plugin.metadata=jimMeta
jim.plugin.sqlengine=mysqlEngine
jim.plugin.sqlexecutor=jimExecutor
jim.plugin.storeengine=jimStore
 
jim.reactor.debug=false
#0:DISABLED,1:SIMPLE,2:ADVANCED,3:PARANOID
jim.netty.leak=1
 
jim.aynctask.threads=32
jim.grpc.threads=8
 
#元数据http地址 master地址
jim.meta.address=http://xx.xx.xx.xx:443
jim.meta.interval=600000
jim.cluster=2
 
####################### Netty Server ##################################################
#服务IP
netty.server.host=0.0.0.0
#服务端口
netty.server.port=3306
#连接请求最大队列长度，如果队列满时收到连接指示，则拒绝该连接。
netty.server.backlog=65536
#默认发送数据包超时时间，默认5秒
netty.server.sendTimeout=5000
#Selector线程
netty.server.bossThreads=1
#IO线程, 0=cpu num
netty.server.ioThreads=8
#通道最大空闲时间(毫秒)
netty.server.maxIdle=1800000
#socket读超时时间(毫秒)
netty.server.soTimeout=3000
#socket缓冲区大小
netty.server.socketBufferSize=16384
#使用EPOLL，只支持Linux模式
netty.server.epoll=true
#协议packet最大值
netty.server.frameMaxSize=16778240
#内存分配器
netty.server.allocatorFactory=
#表示是否允许重用Socket所绑定的本地地址
netty.server.reuseAddress=true
#关闭时候，对未发送数据包等待时间(秒)，-1,0:禁用,丢弃未发送的数据包>0，等到指定时间，如果还未发送则丢弃
netty.server.soLinger=-1
#启用nagle算法，为真立即发送，否则得到确认或缓冲区满发送
netty.server.tcpNoDelay=true
#保持活动连接，定期心跳包
netty.server.keepAlive=true
 
####################### Netty Client ##################################################
#连接池大小
netty.client.poolSize=32
#IO线程数, 0=cpu num, -1=共用serverIO线程
netty.client.ioThreads=4
#连接超时(毫秒)
netty.client.connTimeout=3000
#默认发送数据包超时时间(毫秒)
netty.client.sendTimeout=5000
#socket读超时时间(毫秒)
netty.client.soTimeout=3000
#通道最大空闲时间(毫秒)
netty.client.maxIdle=3600000
#心跳间隔(毫秒)
netty.client.heartbeat=10000
#socket缓冲区大小
netty.client.socketBufferSize=16384
#协议packet最大值
netty.client.frameMaxSize=16778240
#使用EPOLL，只支持Linux模式
netty.client.epoll=true
#内存分配器
netty.client.allocatorFactory=
#关闭时候，对未发送数据包等待时间(秒)，-1,0:禁用,丢弃未发送的数据包>0，等到指定时间，如果还未发送则丢弃
netty.client.soLinger=-1
#启用nagle算法，为真立即发送，否则得到确认或缓冲区满发送
netty.client.tcpNoDelay=true
#保持活动连接，定期心跳包
netty.client.keepAlive=true
row.id.step.size=100000
```

## log4j2.xml

### proxy配置文件

```
<?xml version='1.0' encoding='UTF-8' ?>
<Configuration status="OFF">
    <Properties>
        <Property name="pattern">%d{yyyy-MM-dd HH:mm:ss.fff} [%level] -- %msg%n</Property>
    </Properties>
    <Appenders>
        <Console name="CONSOLE" target="SYSTEM_OUT">
            <PatternLayout>
                <Pattern>${pattern}</Pattern>
            </PatternLayout>
        </Console>
        <RollingRandomAccessFile name="ROLLFILE" immediateFlush="false" bufferSize="256"
                                 fileName="/export/Logs/jimsql/jim-server.log"
                                 filePattern="/export/Logs/jimsql/jim-server.log.%d{yyyy-MM-dd}.%i.gz">
            <PatternLayout>
                <Pattern>${pattern}</Pattern>
            </PatternLayout>
            <Policies>
                <TimeBasedTriggeringPolicy modulate="true" interval="1"/>
            </Policies>
            <DefaultRolloverStrategy max="20">
                <Delete basePath="/export/Logs/jimsql" maxDepth="1">
                    <IfFileName glob="*.gz"/>
                    <IfLastModified age="3d"/>
                </Delete>
            </DefaultRolloverStrategy>
        </RollingRandomAccessFile>
    </Appenders>
    <Loggers>
        <AsyncRoot level="warn" includeLocation="false">
            <AppenderRef ref="ROLLFILE"/>
        </AsyncRoot>
    </Loggers>
</Configuration>
```

## log4j2.component.properties

### proxy配置文件

```
log4j2.asyncLoggerRingBufferSize=1048576
log4j2.asyncLoggerWaitStrategy=Sleep
```

## bin下停启proxy命令

### start.sh — proxy启动脚本

```
#!/bin/sh
 
BASEDIR=`dirname $0`/..
BASEDIR=`(cd "$BASEDIR"; pwd)`
 
export JAVA_HOME=/export/servers/jdk1.8.0_60
 
# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi
 
CLASSPATH="$BASEDIR"/conf/:"$BASEDIR"/lib/*
CONFIG_FILE="$BASEDIR/conf/jim.properties"
echo "$CLASSPATH"
 
if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi
 
 
OPTS_MEMORY=`grep -ios 'opts.memory=.*$' ${CONFIG_FILE} | tr -d '\r'`
OPTS_MEMORY=${OPTS_MEMORY#*=}
 
#DEBUG_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"
 
nohup "$JAVACMD"\
  $OPTS_MEMORY $DEBUG_OPTS \
  -classpath "$CLASSPATH" \
  -Dbasedir="$BASEDIR" \
  -Dfile.encoding="UTF-8" \
  io.chubao.jimdb.server.JimBootstrap &
echo $! > jim.pid
```

### stop.sh — proxy停止脚本

```
#!/bin/sh
if [ "$1" == "pid" ]
then
    PIDPROC=`cat ./jim.pid`
else
    PIDPROC=`ps -ef | grep 'io.chubao.jimdb.server.JimBootstrap' | grep -v 'grep'| awk '{print $2}'`
fi
 
if [ -z "$PIDPROC" ];then
 echo "jim.server is not running"
 exit 0
fi
 
echo "PIDPROC: "$PIDPROC
for PID in $PIDPROC
do
if kill $PID
   then echo "process jim.server(Pid:$PID) was force stopped at " `date`
fi
done
echo stop finished.
```

## proxy启动后

### proxy启动后进程

```
[root@79 bin]# ps -ef|grep jim
root     21234 18113  0 10:10 pts/0    00:00:00 grep --color=auto jim
root     57810     1 99 Sep30 ?        124-18:30:04 /export/servers/jdk1.8.0_60/bin/java -Xms8G -Xmx8G -Xmn3G -XX:SurvivorRatio=8 -XX:MaxDirectMemorySize=4G -XX:MetaspaceSize=64M -XX:MaxMetaspaceSize=512M -Xss256K -server -XX:+TieredCompilation -XX:CICompilerCount=3 -XX:InitialCodeCacheSize=64m -XX:ReservedCodeCacheSize=2048m -XX:CompileThreshold=1000 -XX:FreqInlineSize=2048 -XX:MaxInlineSize=512 -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:CMSMaxAbortablePrecleanTime=100 -XX:+PrintGCDetails -Xloggc:/export/Logs/jimsql/gc.log -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses -XX:+PrintGCTimeStamps -classpath /export/App/jim-server/conf/:/export/App/jim-server/lib/* -Dbasedir=/export/App/jim-server -Dfile.encoding=UTF-8 io.chubao.jimdb.server.JimBootstrap
```

proxy一般按流量大小可以部署一到多个节点，重复上面的步骤可部署多个节点。