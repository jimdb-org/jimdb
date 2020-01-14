// Copyright 2019 The JIMDB Authors.
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

#include <string>
#include <future>
#include <map>

#include "net/context_pool.h"
#include "net/session.h"
#include "dspb/admin.pb.h"

namespace jim {
namespace ds {
namespace tool {

class AdminClient final {
public:
    AdminClient(const std::string& ip, const std::string& port);
    ~AdminClient();

    AdminClient(const AdminClient&) = delete;
    AdminClient& operator=(const AdminClient&) = delete;

    Status Admin(const dspb::AdminRequest& req, dspb::AdminResponse& resp);

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);
    std::shared_ptr<net::Session> getConn();

private:
    using RespFuture = std::future<dspb::AdminResponse>;

    class RequestQueue {
    public:
        std::pair<uint64_t,RespFuture> add();
        void set(uint64_t seq, dspb::AdminResponse&& resp);
        void remove(uint64_t seq);

    private:
        std::map<uint64_t, std::promise<dspb::AdminResponse>> que_;
        std::mutex mu_;
        uint64_t seq_ = 0;
    };

private:
    const std::string ip_;
    const std::string port_;
    net::IOContextPool pool_;

    std::weak_ptr<net::Session> session_;
    std::mutex session_lock_;

    RequestQueue request_queue_;
};

} // namespace jim
} // namespace tool
} // namespace ds
