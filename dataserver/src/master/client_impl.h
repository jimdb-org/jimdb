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
//

_Pragma("once");

#include <string>
#include <vector>
#include <atomic>

#include "cpr/cpr.h"
#include "base/status.h"
#include "proto/gen/mspb/mspb.pb.h"
#include "client.h"

namespace jim {
namespace ds {
namespace master {

class MasterClientImpl final : public MasterClient {
public:
    MasterClientImpl(uint64_t cluster_id, const std::vector<std::string> &ms_addrs);

    Status NodeRegister(mspb::RegisterNodeRequest &req, mspb::RegisterNodeResponse &resp) override;
    Status GetRaftAddress(uint64_t node_id, std::string *addr) override;
    Status NodeHeartbeat(mspb::NodeHeartbeatRequest &req, mspb::NodeHeartbeatResponse &resp) override;
    Status RangeHeartbeat(mspb::RangeHeartbeatRequest &req, mspb::RangeHeartbeatResponse &resp) override;
    Status AskSplit(mspb::AskSplitRequest &req, mspb::AskSplitResponse &resp) override;

private:
    Status post(const std::string &uri,
            const google::protobuf::Message& req, google::protobuf::Message &resp);

    Status nodeGet(mspb::GetNodeRequest &req, mspb::GetNodeResponse &resp);

private:
    const uint64_t cluster_id_ = 0;
    static thread_local cpr::Session session_;
    std::vector<std::string> ms_addrs_;
    std::atomic<uint64_t> rr_counter_ = {0};
};

} // namespace master
} // namespace ds
} // namespace jim
