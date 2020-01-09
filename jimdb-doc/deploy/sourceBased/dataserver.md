# DataServer部署

## 编译代码：

切换代码库master分支，进入data-server目录，执行编译脚本build.sh，在build目录中生成可执行文件data-server：

启动服务：

 ```
ulimit -c unlimited
./data-server ds.conf start
 ```
 
## 配置说明：

* base_path：data-server可执行文件所在目录

    > 例如：

    > base_path = /export/data-server/    # 注：在启动命令中，指定配置文件路径是相对于此base_path的

* rocksdb段的配置：指定磁盘存储路径。注：目前mass tree内存版本不是用此配置

    > 例如：

    > [rocksdb]

    > path = /export/data-server/data/db

* heartbeart段配置：指定此ds的元数据服务master地址和心跳频率：

    > 例如，如果master只有1个副本：

    > master_num= 1    # 指定master server服务的副本数量

    > master_host= “192.168.0.1:8081” # 指定master server服务的rpc地址

    > ***

    > 再例如，如果master server有3个副本，则要依依列出master_host:

    > master_num= 3

    > master_host= “192.168.0.1:8081”

    > master_host= “192.168.0.2:8081”

    > master_host= “192.168.0.3:8081”

    > node_heartbeat_interval = 10          # data-server node心跳时间间隔

    > range_heartbeat_interval= 10 # da ta-server range 心跳时间间隔

* log段配置：指定日志路径和级别

    > log_path = /export/data-server/log

    > log_level = info             # 可以是debug info warn error

* worker段配置：指定io工作线程服务的端口和线程数量

    > 例如：

    > [worker]

    > port = 9090                   # 工作线程服务的rpc端口，比如sql请求会发到此端口

* manager段配置：指定管理线程服务的端口

    > 例如：

    > [manger]

    > port.= 9091                   # 管理线程服务的rpc端口，比如创建分片请求会发到此端口

* range段配置：指定分片分裂阈值

    > 例如：

    > [range]

    > check_size = 128MB               # 触发range分裂检查的阈值，即大于此阈值后开始检测

    > split_size = 256MB                   # 指定分裂range的大小，通常为max_size的一半

    > max_size = 512MB                  # 指定range分裂的阈值，即等于此阈值开始分裂

* raft段配置:指定raft使用的端口和raft 日志路径

    > 例如：

    > [raft]

    > port = 9092                                                                           # raft 使用的端口

    > log_path = /export/data-server/data/raft   # raft日志的存储路径

