Deployment
=============================

Master Deployment
--------------------------

Compilation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We can get an executable file for the master by executing the following script

.. code-block:: bash

	cd jimdb/master-server/cmd
	./build.sh


Starting the Service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Before starting the master, a config file named ms.conf needs to be provided. Using the following command to start the master

.. code-block:: bash

	setsid ./master-server -config ms.conf &


Configuration (ms.conf) Instructions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* node-id: master-server's id in Raft replication group, same as id of cluster.peer ip

::

	e.g.
	node-id = 1

* data-dir: the storage directory for master to save cluster meta data

::

	e.g.
	data-dir = “/export/master-server/data”

* cluster: cluster-id, cluster's id which master-server act on

:: 

	e.g.
	[cluster]
	cluster-id = 10

* cluster: peer, master-service provides high-availability metadata service capabilities by uses raft to implement metadata replication. peer is an Array, which describes the information of all members of the raft replication group, include:

  + id(the amount of this master in the replication group)

  + host (master ip)

  + http-port (http service port provided by the master, mainly serving the web console side)

  + rpc-port (rpc port provided by the master, mainly serving the gateway)

  + raft-ports ( port for raft heartbeat and replication)

::

	e.g.

	master-server for one copy

	[[cluster.peer]]

	id = 1  # replication group id for master-server node 

	host = “192.168.0.1”  # master-server IP

	http-port = 8080  # http port 8080

	rpc-port = 8081  # rpc port 8081

	raft-ports = [8082,8083]  # raft heartbeat port 8082，copy port 8083

	e.g.

	master-server for three copies:

	[[cluster.peer]]

	id = 1  # first replication group id for master-server node

	host = “192.168.0.1”  # master-server IP 192.168.0.1

	http-port = 8080

	rpc-port = 8081

	raft-ports = [8082,8083]

	[[cluster.peer]]

	id = 2  # second replication group id for master-server node

	host = “192.168.0.2”  # master-server IP 192.168.0.2

	http-port = 8080

	rpc-port = 8081

	raft-ports = [8082,8083]

	[[cluster.peer]]

	id = 3  # third replication group id for master-server node

	host = “192.168.0.3”  # master-server IP 192.168.0.3

	http-port = 8080

	rpc-port = 8081

	raft-ports = [8082,8083]

* log: the log directory, prefix and level of log file

::

	e.g.
	[log]

	dir = “/export/master-server/log”

	module = “master”

	level = “info”  # value can be debug, info, warn, error

* replication: amount of data range copies when create data table

::

	e.g.
	[replication]

	max-replicas = 1  # one copy, it will be three copies if the value is 3


Data-Server Deployment
------------------------

Compilation
^^^^^^^^^^^^^^^^^^
Before starting the data server, a config file named ds.conf needs to be provided. Using the following command to start data server

.. code-block:: bash

	ulimit -c unlimited
	./data-server ds.conf start


Configuration (ds.conf) Instructions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* base_path: executable file's directory

::

	e.g.
	base_path = /export/data-server/  Note. config file's directory is relative to this base_path when execute start cmd

* rocksdb: the directory for disk storage.  Note. not for mass tree memory version

::

	e.g.
	[rocksdb]
	path = /export/data-server/data/db

* heartbeart: master ip and heart frequency for ds meta data service

::

	e.g.
	
	for 1 copy:
	master_num= 1  # master server copy amount
	master_host= “192.168.0.1:8081” # master server rpc ip

	eg.
	for 3 copoes
	master_num= 3
	master_host= “192.168.0.1:8081”
	master_host= “192.168.0.2:8081”
	master_host= “192.168.0.3:8081”
	node_heartbeat_interval = 10  # data-server node heartbeat interval 
	range_heartbeat_interval= 10  # data-server range heartbeat interval 

* log: directory and level of log file

:: 

	e.g.
	[log]
	log_path = /export/data-server/log
	log_level = info  # value can be debug, info, warn, error

* worker: io worker thread port and threads amount

::

	e.g.
	[log]
	log_path = /export/data-server/log
	log_level = info  # value can be debug, info, warn, error

* manager: threads manage port

::

	e.g.
	[manger]
	port.= 9091  # threads manage rpc port, eg., create range request will get to this port

* range: split threshold

::

	e.g.
	[range]
	check_size = 128MB  # threshold to trigger range split detection
	split_size = 256MB  # size of range split,  usually half of max_size
	max_size = 512MB  # threshold for range split, will split when equal or more than this value

* raft: raft port and raft log directory

::

	e.g.
	[raft]
	port = 9092  # raft port
	log_path = /export/data-server/data/raft # raft log directory
                                                 

Proxy Deployment
------------------

Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

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

Config File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
jim.properties

::

	opts.memory=-Xms8G -Xmx8G -Xmn3G -XX:SurvivorRatio=8 -XX:MaxDirectMemorySize=4G -XX:MetaspaceSize=64M -XX:MaxMetaspaceSize=512M -Xss256K -server -XX:+TieredCompilation -XX:CICompilerCount=3 -XX:InitialCodeCacheSize=64m -XX:ReservedCodeCacheSize=2048m -XX:CompileThreshold=1000 -XX:FreqInlineSize=2048 -XX:MaxInlineSize=512 -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:CMSMaxAbortablePrecleanTime=100 -XX:+PrintGCDetails -Xloggc:/export/Logs/jimsql/gc.log -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses -XX:+PrintGCTimeStamps

	# JIM
	jim.outbound.threads=0
	jim.inbound.threads=0
	jim.plugin.metadata=jimMeta
	jim.plugin.sqlengine=mysqlEngine
	jim.plugin.sqlexecutor=jimExecutor
	jim.plugin.storeengine=jimStore

	jim.reactor.debug=false
	# 0:DISABLED,1:SIMPLE,2:ADVANCED,3:PARANOID
	jim.netty.leak=1

	jim.aynctask.threads=32
	jim.grpc.threads=8

	# meta data http ip, master ip 
	jim.meta.address=http://xx.xx.xx.xx:443
	jim.meta.interval=600000
	jim.cluster=2

	####################### Netty Server ##################################################
	# server IP
	netty.server.host=0.0.0.0
	#server Port
	netty.server.port=3306
	# max queue for connection request, refuse coming request when it is full
	netty.server.backlog=65536
	# default timeout for send data, default 5s
	netty.server.sendTimeout=5000
	# Selector thread
	netty.server.bossThreads=1
	# IO thread, 0=cpu num
	netty.server.ioThreads=8
	# max channel idle time millisecond
	netty.server.maxIdle=1800000 ms
	# socket timeout for read (ms)
	netty.server.soTimeout=3000
	# socket buffer size
	netty.server.socketBufferSize=16384
	# use EPOLL，support Linux mode only
	netty.server.epoll=true
	# protocol packet max
	netty.server.frameMaxSize=16778240
	# memory allocator
	netty.server.allocatorFactory=
	# allow or not reuse Socket bound local address 
	netty.server.reuseAddress=true
	# waiting time(s) for unsend data packet when close. -1,0: disable, discard; >0: wait until schedule time, discard if not send yet
	netty.server.soLinger=-1
	# open nagle, send immediately when it's true, otherwise will send when confirm or buffer is full
	netty.server.tcpNoDelay=true
	# keep active connect, regular heartbeat packet
	netty.server.keepAlive=true

	####################### Netty Client ##################################################
	# connect pool size
	netty.client.poolSize=32
	# IO thread, 0=cpu num, -1= share serverIO thread
	netty.client.ioThreads=4
	# connect timeout (ms)
	netty.client.connTimeout=3000
	# default timeout for send data packet (ms)
	netty.client.sendTimeout=5000
	# socket read timeout(ms)
	netty.client.soTimeout=3000
	# max channel idle time(ms)
	netty.client.maxIdle=3600000
	# heartbeat interval(ms)
	netty.client.heartbeat=10000
	# socket buffer size
	netty.client.socketBufferSize=16384
	# protocol packet max
	netty.client.frameMaxSize=16778240
	# use EPOLL，support Linux mode only
	netty.client.epoll=true
	# memory allocator
	netty.client.allocatorFactory=
	# waiting time(s) for unsend data packet when close. -1,0: disable, discard; >0: wait until schedule time, discard if not send yet
	netty.client.soLinger=-1
	# open nagle, send immediately when it's true, otherwise will send when confirm or buffer is full
	netty.client.tcpNoDelay=true
	# keep active connect, regular heartbeat packet
	netty.client.keepAlive=true
	row.id.step.size=100000


log4j2.xml

.. code-block:: xml

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

log4j2.component.properties

::

	log4j2.asyncLoggerRingBufferSize=1048576
	log4j2.asyncLoggerWaitStrategy=Sleep

start and stop cmd
^^^^^^^^^^^^^^^^^^^^

start.sh

::

	# !/bin/sh

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

	# DEBUG_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"

	nohup "$JAVACMD"\
	  $OPTS_MEMORY $DEBUG_OPTS \
	  -classpath "$CLASSPATH" \
	  -Dbasedir="$BASEDIR" \
	  -Dfile.encoding="UTF-8" \
	  io.jimdb.server.JimBootstrap &
	echo $! > jim.pid


stop.sh

::

	# !/bin/sh
	if [ "$1" == "pid" ]
	then
	    PIDPROC=`cat ./jim.pid`
	else
	    PIDPROC=`ps -ef | grep 'io.jimdb.server.JimBootstrap' | grep -v 'grep'| awk '{print $2}'`
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

after start.sh

.. code-block:: bash 

	[root@79 bin]# ps -ef|grep jim
	root     21234 18113  0 10:10 pts/0    00:00:00 grep --color=auto jim
	root     57810     1 99 Sep30 ?        124-18:30:04 /export/servers/jdk1.8.0_60/bin/java -Xms8G -Xmx8G -Xmn3G -XX:SurvivorRatio=8 -XX:MaxDirectMemorySize=4G -XX:MetaspaceSize=64M -XX:MaxMetaspaceSize=512M -Xss256K -server -XX:+TieredCompilation -XX:CICompilerCount=3 -XX:InitialCodeCacheSize=64m -XX:ReservedCodeCacheSize=2048m -XX:CompileThreshold=1000 -XX:FreqInlineSize=2048 -XX:MaxInlineSize=512 -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:CMSMaxAbortablePrecleanTime=100 -XX:+PrintGCDetails -Xloggc:/export/Logs/jimsql/gc.log -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses -XX:+PrintGCTimeStamps -classpath /export/App/jim-server/conf/:/export/App/jim-server/lib/* -Dbasedir=/export/App/jim-server -Dfile.encoding=UTF-8 io.jimdb.server.JimBootstrap

Reminder
^^^^^^^^^^^^^^^^^^^^^^^^^^

We can have multiple Proxys in JIMDB. The size of proxy depends on the throughput of the deployed clusters. Each Proxy needs to be deployed by following the above steps.
