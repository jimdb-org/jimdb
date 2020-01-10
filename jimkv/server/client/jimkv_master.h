
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

#include <string>
#include <vector>
#include <memory>
#include <functional>

#include "base/status.h"
#include "mspb/mspb.pb.h"

namespace jimkv {
namespace sdk {

using Req = google::protobuf::Message;
using Resp = google::protobuf::Message;


using RoutePred = std::function<bool(mspb::GetRouteResponse&)>;
using NodePred = std::function<bool(mspb::GetNodeResponse&)>;

class JimMaster final {
public:
    JimMaster(uint64_t cluster_id, const std::string& host);

    bool GetRoute(mspb::GetRouteRequest& req, RoutePred pred);
    bool GetNode(uint64_t node_id, NodePred pred);

private:
    bool Post(const std::string &uri, const Req& req, Resp *resp);
    bool CheckHeader(const mspb::ResponseHeader& header);

private:
    const uint64_t cluster_id_;
    const std::string host_;
};

using MasterPtr = std::shared_ptr<JimMaster>;

} // namespace sdk
} // namespace jimkv

