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

_Pragma ("once");

#include <set>
#include <map>
#include <memory>

#include "base/shared_mutex.h"
#include "net/context_pool.h"

#include "jim_cluster.h"
#include "jim_assist.h"
#include "jim_command.h"

namespace jim {
namespace sdk {

using IOContextPoolPtr = std::shared_ptr<net::IOContextPool>;

class JimClient final {
public:
    ~JimClient() = default;

    JimClient(const JimClient&) = delete;
    JimClient& operator=(const JimClient&) = delete;

    static JimClient& Instance() {
        static JimClient instance;
        return instance;
    }

    void Start(int thread_num, std::string name);
    void Stop();
    bool IsRun() {return running_.load();}

    ClusterPtr AddCluster(uint64_t cluster_id, std::string &host, uint32_t conn_count = 5);
    ClusterPtr GetCluster(uint64_t cluster_id);
    void RemoveCluster(uint64_t cluster_id);

    bool SendCmd(CmdPtr cmd, int timeout);

    AssistPtr GetAssist() {return assist_;}

    IOContextPoolPtr GetIOContextPool() {return context_pool_;}
private:
    JimClient() = default;

private:
    std::atomic<bool> running_;

    AssistPtr assist_;
    IOContextPoolPtr context_pool_;

    std::map<uint64_t, ClusterPtr> clusters_;
    shared_mutex clusters_lock_;
};

} // namespace sdk
} // namespace jim
