# Master部署

## 编译代码：

切换代码库master分支，进入master-server的cmd目录，执行编译脚本build.sh，生成可执行文件master-server：

 ```
cd chubaodb/master-server/cmd
./build.sh
 ```
 
## 启动服务：

指定配置文件作为参数，运行master-server在后台：
 
 ```
setsid ./master-server -config ms.conf &
 ```

## 配置说明：

* node-id：此master-server在raft复制组中的id，即cluster.peer段中的ip相同的id

    > 例如：

    > node-id = 1

* data-dir：master保存集群元数据的存储目录，

    > 例如：

    > data-dir = “/export/master-server/data”

* cluster段的cluster-id配置：指定此master-server所服务的集群id，

    > 例如：

    > [cluster]

    > cluster-id = 10

* cluster段的peer配置：master-server为提供高可用元数据服务能力，使用raft实现元数据复制；peer配置描述的是所有raft复制组成员的信息，peer配置是一个数组，包括id（此master在复制组中的编号）、host（master ip 地址）、http-port（master提供的http服务端口，主要向web管理端服务）、rpc-port（master提供的rpc端口，主要向网关服务）、raft-ports（master使用的raft心跳端口和复制端口）

    > 例如：

    > 配置1副本的master-server：

    > [[cluster.peer]]

    > id = 1                                              # master-server节点复制组id

    > host = “192.168.0.1”           # master-server IP地址

    > http-port = 8080                    # http端口8080

    > rpc-port = 8081                     # rpc端口8081

    > raft-ports = [8082,8083] # raft心跳端口8082，复制端口8083

    > ***

    > 再例如，如果配置3副本的master-server：

    > [[cluster.peer]]

    > id = 1                                              # master-server节点复制组id 1

    > host = “192.168.0.1”           # master-server IP地址 192.168.0.1

    > http-port = 8080                   

    > rpc-port = 8081                    

    > raft-ports = [8082,8083]

    > [[cluster.peer]]

    > id = 2                                              # master-server节点复制组id 2

    > host = “192.168.0.2”           # master-server IP地址192.168.0.2

    > http-port = 8080                   

    > rpc-port = 8081                    

    > raft-ports = [8082,8083]

    > [[cluster.peer]]

    > id = 3                                              # master-server节点复制组id 3

    > host = “192.168.0.3”           # master-server IP地址192.168.0.3

    > http-port = 8080                   

    > rpc-port = 8081                    

    > raft-ports = [8082,8083]


* log段的配置：指定日志目录，日志文件名前缀和日志级别

    > 例如：

    > [log]

    > dir = “/export/master-server/log”

    > module = “master”

    > level = “info”                      # 日志级别可以是debug info warn error


* replication段的配置：指定创建表时分片的副本数量

    > 例如：

    > [replication]

    > max-replicas = 1           # 创建1副本range。如果为3则为创建3副本分片

 


