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

#include "base/status.h"
#include "proto/gen/mspb/mspb.pb.h"

namespace jimkv {
namespace ds {
namespace master {

class MasterClient {
public:
    virtual ~MasterClient() = default;

    virtual Status NodeRegister(mspb::RegisterNodeRequest &req, mspb::RegisterNodeResponse &resp) = 0;
    virtual Status GetRaftAddress(uint64_t node_id, std::string *addr) = 0;
    virtual Status NodeHeartbeat(mspb::NodeHeartbeatRequest &req, mspb::NodeHeartbeatResponse &resp) = 0;
    virtual Status RangeHeartbeat(mspb::RangeHeartbeatRequest &req, mspb::RangeHeartbeatResponse &resp) = 0;
    virtual Status AskSplit(mspb::AskSplitRequest &req, mspb::AskSplitResponse &resp) = 0;
};

} // namespace master
} // namespace ds
} // namespace jimkv
