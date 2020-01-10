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

#include "rpc_request_mock.h"

#include "base/util.h"

namespace jimkv {
namespace test {
namespace mock {

void RPCFuture::Set(const google::protobuf::Message& resp) {
    if (replied_) {
        throw std::logic_error("already replied");
    }
    replied_ = true;
    resp.SerializeToString(&reply_data_);
}

Status RPCFuture::Get(google::protobuf::Message& resp) {
    if (!replied_) {
        return Status(Status::kNotFound, "not reply yet", "");
    }
    if (!resp.ParseFromString(reply_data_)) {
        return Status(Status::kCorruption, "parse proto response", EncodeToHex(reply_data_));
    }
    return Status::OK();
}


RPCRequestMock::RPCRequestMock(const google::protobuf::Message& req,
               const RPCFuturePtr& future,
               dspb::FunctionID func_id):
    RPCRequest(net::Context(), net::NewMessage()),
    future_(future) {
    msg->head.func_id = func_id;
    auto &body = msg->body;
    body.resize(req.ByteSizeLong());
    req.SerializeToArray(body.data(), static_cast<int>(body.size()));
    // TODO:
    auto schedule_req_ptr = dynamic_cast<const dspb::SchRequest*>(&req);
    if (schedule_req_ptr) {
        sch_req.reset(new dspb::SchRequest(*schedule_req_ptr));
    }
    auto range_req_ptr = dynamic_cast<const dspb::RangeRequest*>(&req);
    if (range_req_ptr) {
        range_req.reset(new dspb::RangeRequest(*range_req_ptr));
    }
}

void RPCRequestMock::Reply(const google::protobuf::Message& proto_resp) {
    future_->Set(proto_resp);
}


std::pair<RPCRequestPtr, RPCFuturePtr> NewMockRPCRequest(
        const google::protobuf::Message& proto_req,
        dspb::FunctionID func_id) {
    auto future = std::make_shared<RPCFuture>();
    RPCRequestPtr rpc(new RPCRequestMock(proto_req, future, func_id));
    return std::make_pair(std::move(rpc), future);
}

} // namespace mock
} // namespace test
} // namespace jimkv
