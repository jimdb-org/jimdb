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

_Pragma("once");

#include "master/client.h"

namespace jim {
namespace test {
namespace mock {

using namespace jim::ds;

class MasterClientMock: public master::MasterClient {
public:
    Status NodeRegister(mspb::RegisterNodeRequest &req, mspb::RegisterNodeResponse &resp) override {
        resp.set_node_id(1);
        return Status::OK();
    }

    Status GetRaftAddress(uint64_t node_id, std::string *addr) override {
        return Status(Status::kNotSupported);
    }

    Status NodeHeartbeat(mspb::NodeHeartbeatRequest &req, mspb::NodeHeartbeatResponse &resp) override {
        return Status::OK();
    }

    Status RangeHeartbeat(mspb::RangeHeartbeatRequest &req, mspb::RangeHeartbeatResponse &resp) override {
        return Status::OK();
    }

    Status AskSplit(mspb::AskSplitRequest &req, mspb::AskSplitResponse &resp) override {
        return Status(Status::kNotSupported);
    }
};

}
}
}
