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

namespace jimkv {
namespace net {

class Statistics {
public:
    std::atomic<int64_t> session_count = {0};
    std::atomic<int64_t> recv_msg_count = {0};
    std::atomic<int64_t> recv_bytes_count = {0};
    std::atomic<int64_t> sent_msg_cout = {0};
    std::atomic<int64_t> sent_bytes_count = {0};

    void AddSessionCount(int64_t deta) {
        session_count += deta;
    }

    void AddMessageRecv(int64_t size) {
        ++recv_msg_count;
        recv_bytes_count += size;
    }

    void AddMessageSent(int64_t size) {
        ++sent_msg_cout;
        sent_bytes_count += size;
    }
};


}  // namespace net
}  // namespace jimkv
