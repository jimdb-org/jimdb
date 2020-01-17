
# JIMDB Third Party Dependencies

JIMDB depends on third party libraries to implement some functionality. This document describes which libraries are depended upon, and how. It is maintained by and for humans, and so while it is a best effort attempt to describe the server’s dependencies, it is subject to change as libraries are added or removed.

## Dataserver's Dependencies
    
| name          | License                                  | dependency type | modify                 |
|---------------|------------------------------------------|-----------------|------------------------|
| RocksDB       | GPLv2, Apache 2\.0                       | so              | N                      |
| asio          | Boost Software License \- Version 1.0    | import          | N                      |
| jemalloc      | BSD\-2\-Clause                           | so              | N                      |
| protobuf      | BSD\-3\-Clause                           | so              | N                      |
| tbb           | Apache 2\.0                              | so              | N                      |
| masstree-beta | MIT | so                                 | import          | N                      |
| gtest         | BSD 3\-Clause | so                       | so              | N                      |
| benchmark     | Apache\-2.0 | so                         | so              | N                      |
| cpr           | MIT  | so                                | so              | N                      |
| inih          | BSD\-2\-Clause  | so                     | import          | N                      |
| rapidjson     | MIT | so                                 | import          | N                      |
| spdlog        | MIT | so                                 | import          | N                      |
| librdkafka    | BSD\-2\-Clause | so                      | import          | N                      |
| sql-parser    | MIT | so                                 | so              | N                      |


## Master's Dependencies

| name          | License          | dependency type | modify |
|---------------|------------------|-----------------|------------------------|
| go            | MIT              | import          | N                      |
| ratelimmit    | LGPL\-3.0\-only  | so              | N                      |
| crypto        | BSD\-3\-Clause   | import          | N                      |
| protobuf      | BSD\-3\-Clause   | import          | N                      |
| mux           | BSD\-3\-Clause   | import          | N                      |
| atomic        | MIT              | import          | N                      |
| errors        | BSD\-2\-Clause   | import          | N                      |
| gocron        | BSD\-2\-Clause   | import          | N                      |
| gin           | MIT              | import          | N                      |
| etcd          | Apache\-2\.0     | import          | N                      |
| go\-cache     | MIT              | import          | N                      |
| gotest\.tools | Apache\-2\.0     | import          | N                      |
| grpc\-go      | Apache\-2\.0     | import          | N                      |
| net           | BSD\-3\-Clause   | import          | N                      |
| cast          | MIT              | import          | N                      |

## Redis Proxy's Dependencies

| name          | License          | dependency type | modify  |
|---------------|------------------|-----------------|------------------------|
| redis         | BSD\-3\-Clause   | import          | N                      |
| jemalloc      | BSD\-2\-Clause   | so              | N                      |




