# 使用Docker部署单机测试版

## 前提

运行环境需要安装docker与docker-compose
```
安装docker
  mac安装方式：
  brew install docker docker-compose
  其他系统安装方式：
  https://docs.docker.com/install/linux/docker-ce/centos/

启动docker

```

## 获取文件

```
下载相关脚本代码包
wget http://storage.jd.local/docker-compose/chubaodb_docker_compose.1126.tar.gz
//mac的wget安装：brew install wget

解压文件
tar -zxf chubaodb_docker_compose.1126.tar.gz

跳转目录
cd chubaodb_docker_compose/
```

## 部署运行
```
[root@ap2 chubaodb_docker_compose]# ./stop.sh
Stopping chubaodb_docker_compose_ms_1 ... done
Removing chubaodb_docker_compose_ds_1 ... done
Removing chubaodb_docker_compose_ms_1 ... done
Removing network chubaodb_docker_compose_default

[root@ap2 chubaodb_docker_compose]# ./start.sh
Creating network "chubaodb_docker_compose_default" with the default driver
Creating chubaodb_docker_compose_ms_1 ... done
Creating chubaodb_docker_compose_ds_1 ... done
Creating chubaodb_docker_compose_gw_1 ... done

[root@ap2 chubaodb_docker_compose]# docker ps
CONTAINER ID        IMAGE                        COMMAND                  CREATED             STATUS              PORTS                                                                        NAMES
77d8423410a6        chubaodb_docker_compose_gw   "./entrypoint.sh -cl…"   About an hour ago   Up About an hour    0.0.0.0:3361->3361/tcp                                                       chubaodb_docker_compose_gw_1
5fdf148b42fa        chubaodb_docker_compose_ds   "./entrypoint.sh -cl…"   About an hour ago   Up About an hour    0.0.0.0:6182->6182/tcp, 0.0.0.0:16182->16182/tcp, 0.0.0.0:18881->18881/tcp   chubaodb_docker_compose_ds_1
b1189f252ae0        chubaodb_docker_compose_ms   "./entrypoint.sh -cl…"   About an hour ago   Up About an hour    0.0.0.0:8811->8811/tcp                                                       chubaodb_docker_compose_ms_1
```

## 使用
```
1 curl向master 发命令建库建表 // master 端口见docker-compose.yml
2 mysql -P xxxx  -u root // 密码也是root 端口见docker-compose.yml

#db创建
curl -XPOST  -d '{"header": {"cluster_id": 10},"name":"jimtesthot"}' http://0.0.0.0:8817/db/create | python -m json.tool

#删除表
curl -XPOST  -d '{"header": {"cluster_id": 10},"db_name":"jimtesthot","table_name":"test"}' http://0.0.0.0:8817/table/delete | python -m json.tool

建表
curl -XPOST  -d '{"header": {"cluster_id": 10},"table_name":"test","db_name":"jimtesthot","replica_num":1,"data_range_num":2,"data_doc_num":500000,"type":2,"properties":"{\"columns\":[{\"name\":\"id\",\"data_type\":4,\"primary_key\":1},{\"name\":\"col01\",\"data_type\":4},{\"name\":\"col02\",\"data_type\":3},{\"name\":\"col03\",\"data_type\":3},{\"name\":\"col04\",\"data_type\":3},{\"name\":\"col05\",\"data_type\":7},{\"name\":\"col06\",\"data_type\":7},{\"name\":\"col07\",\"data_type\":7},{\"name\":\"col08\",\"data_type\":7},{\"name\":\"col09\",\"data_type\":7},{\"name\":\"col10\",\"data_type\":7},{\"name\":\"col11\",\"data_type\":7},{\"name\":\"col12\",\"data_type\":7},{\"name\":\"col13\",\"data_type\":7},{\"name\":\"col14\",\"data_type\":7},{\"name\":\"col15\",\"data_type\":7},{\"name\":\"col16\",\"data_type\":7},{\"name\":\"col17\",\"data_type\":7},{\"name\":\"col18\",\"data_type\":7},{\"name\":\"col19\",\"data_type\":7},{\"name\":\"col20\",\"data_type\":7},{\"name\":\"col21\",\"data_type\":7},{\"name\":\"col22\",\"data_type\":7},{\"name\":\"col23\",\"data_type\":7},{\"name\":\"col24\",\"data_type\":7},{\"name\":\"col25\",\"data_type\":7},{\"name\":\"col26\",\"data_type\":7},{\"name\":\"col27\",\"data_type\":7},{\"name\":\"col28\",\"data_type\":7},{\"name\":\"col29\",\"data_type\":7},{\"name\":\"col30\",\"data_type\":7},{\"name\":\"col31\",\"data_type\":7}],\"indexes\":[{\"name\":\"unqe_col01\",\"col_names\":[\"col01\"],\"unique\":true},{\"name\":\"unqe_col02\",\"col_names\":[\"col02\"],\"unique\":true},{\"name\":\"unqe_col03\",\"col_names\":[\"col03\"],\"unique\":true},{\"name\":\"unqe_col04\",\"col_names\":[\"col04\"],\"unique\":true},{\"name\":\"unqe_col05\",\"col_names\":[\"col05\"],\"unique\":true},{\"name\":\"unqe_col16\",\"col_names\":[\"col16\"]},{\"name\":\"unqe_col17\",\"col_names\":[\"col17\"]},{\"name\":\"unqe_col28\",\"col_names\":[\"col28\"]},{\"name\":\"unqe_col29\",\"col_names\":[\"col29\"]},{\"name\":\"unqe_col10\",\"col_names\":[\"col10\"]},{\"name\":\"unqe_col11\",\"col_names\":[\"col11\"]}]}"}'  http://0.0.0.0:8817/table/create | python -m json.tool
```
