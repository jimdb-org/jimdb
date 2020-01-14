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

#include <queue>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "dspb/raft_internal.pb.h"
#include "common/rpc_request.h"

namespace jim {
namespace ds {
namespace range {

static const uint64_t kPassbySubmitQueueCmdSeq = 0;

class SubmitContext final {
public:
    SubmitContext(RPCRequestPtr rpc_request, dspb::CmdType type,
            dspb::RangeRequest_Header&& req_header);

    ~SubmitContext() = default;

    SubmitContext(const SubmitContext&) = delete;
    SubmitContext& operator=(const SubmitContext&) = delete;

    int64_t SubmitTime() const { return submit_time_; }
    dspb::CmdType Type() const { return type_; }

    void SendResponse(dspb::RangeResponse& resp, ErrorPtr err);

    void SendError(ErrorPtr err);

    void CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs);

    uint64_t MsgID() const;

private:
    int64_t submit_time_ = 0;
    RPCRequestPtr rpc_request_;
    dspb::CmdType type_;

    // save for set response header lately
    dspb::RangeRequest_Header req_header_;
};

class SubmitQueue {
public:
    SubmitQueue() = default;
    ~SubmitQueue() = default;

    SubmitQueue(const SubmitQueue&) = delete;
    SubmitQueue& operator=(const SubmitQueue&) = delete;

    uint64_t GetSeq();

    uint64_t Add(RPCRequestPtr rpc_request, dspb::CmdType type,
            dspb::RangeRequest_Header&& req_header);

    std::unique_ptr<SubmitContext> Remove(uint64_t seq_id);

    std::vector<std::unique_ptr<SubmitContext>> ClearAll();

    std::vector<uint64_t> GetExpired(size_t max_count = 10000);

    size_t Size() const;

    void Shrink();

private:
    using SubmitContextPtr = std::unique_ptr<SubmitContext>;
    using ContextMap = std::unordered_map<uint64_t, SubmitContextPtr>;
    using ExpirePair = std::pair<time_t, uint64_t>;

    class ExpireQueue : public std::priority_queue<ExpirePair, std::vector<ExpirePair>, std::greater<ExpirePair>> {
    public:
        std::vector<ExpirePair>& GetContainer() { return this->c; }
        void Clear() { this->c.clear(); }
    };

    uint64_t seq_ = 0;
    ContextMap ctx_map_;
    ExpireQueue expire_que_;
    mutable std::mutex mu_;
};

}  // namespace range
}  // namespace ds
}  // namespace jim
