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

#include "common/rpc_request.h"
#include "base/status.h"
#include "dspb/function.pb.h"

namespace jim {
namespace test {
namespace mock {

class RPCFuture {
public:
    void Set(const google::protobuf::Message& resp);
    Status Get(google::protobuf::Message& resp);

private:
    bool replied_ = false;
    std::string reply_data_;
};

using RPCFuturePtr = std::shared_ptr<RPCFuture>;

class RPCRequestMock: public RPCRequest {
public:
    RPCRequestMock(const google::protobuf::Message& req,
            const RPCFuturePtr& result,
            dspb::FunctionID func_id = dspb::kFuncRangeRequest);

    void Reply(const google::protobuf::Message& proto_resp) override;

private:
    RPCFuturePtr future_;
};

// return a RPCRequest and a RPCFuture for fetching result
std::pair<RPCRequestPtr, RPCFuturePtr> NewMockRPCRequest(
        const google::protobuf::Message& proto_req,
        dspb::FunctionID func_id = dspb::kFuncRangeRequest);

} // namespace mock
} // namespace test
} // namespace jim
