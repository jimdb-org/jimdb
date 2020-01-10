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

#include <vector>
#include <google/protobuf/message.h>

#include "net/message.h"
#include "dspb/api.pb.h"
#include "dspb/schedule.pb.h"

namespace jimkv {

struct RPCRequest {
    net::Context ctx;
    net::MessagePtr msg;
    int64_t expire_time = 0; // absolute time, unit: ms
    int64_t begin_time = 0;  // unit: us

    // request protobuf message
    std::unique_ptr<dspb::RangeRequest> range_req;
    std::unique_ptr<dspb::SchRequest> sch_req;

    RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg);
    virtual ~RPCRequest() = default;

    uint64_t MsgID() const { return msg->head.msg_id; }
    std::string FuncName() const;

    // parse to request protobuf msg
    bool Parse(bool zero_copy = true);

    // reply response to client, declare virtual for mock
    virtual void Reply(const google::protobuf::Message& proto_resp);
};

using RPCRequestPtr = std::unique_ptr<RPCRequest>;

using ErrorPtr = std::unique_ptr<dspb::Error>;

// set RangeResponse_Header from RangeRequest_Header
void SetResponseHeader(dspb::RangeResponse_Header* resp,
                       const dspb::RangeRequest_Header &req, ErrorPtr err = nullptr);

} // namespace jimkv
