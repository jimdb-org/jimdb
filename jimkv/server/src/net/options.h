// Copyright 2019 The JimDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

_Pragma("once");

#include "statistics.h"

namespace jimkv {
namespace net {

struct SessionOptions {
    // connection read timeout
    // a zero value means no timeout
    size_t read_timeout_ms = 5000;

    // connection write request timeout
    size_t write_timeout_ms = 5000;

    // max pending writes per connection
    // TODO: limit memory usage
    size_t write_queue_capacity = 2000;

    // allowed max packet length when read
    size_t max_packet_length = 10 << 20;

    std::shared_ptr<Statistics> statistics;
};

struct ServerOptions {
    // how many threads will server connections use
    // zero value means share with the accept thread
    size_t io_threads_num = 4;

    // exceeded connections will be rejected
    int64_t max_connections = 50000;

    // options about session
    SessionOptions session_opt;
};

}  // namespace net
}  // namespace jimkv
