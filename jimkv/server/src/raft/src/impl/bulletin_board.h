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

#include <atomic>
#include <mutex>
#include <vector>
#include "base/shared_mutex.h"
#include "raft.pb.h"
#include "raft/status.h"
#include "raft/types.h"

namespace jimkv {
namespace raft {
namespace impl {

// BulletinBoard: exhibit fsm's newest status
class BulletinBoard {
public:
    BulletinBoard() = default;
    ~BulletinBoard() = default;

    BulletinBoard(const BulletinBoard&) = delete;
    BulletinBoard& operator=(const BulletinBoard&) = delete;

    void PublishLeaderTerm(uint64_t leader, uint64_t term);
    void PublishPeers(std::vector<Peer>&& peers);
    void PublishStatus(RaftStatus&& status);

    uint64_t Leader() const { return leader_; }
    uint64_t Term() const { return term_; }
    void LeaderTerm(uint64_t* leader, uint64_t* term) const;

    void Peers(std::vector<Peer>* peers) const;

    void Status(RaftStatus* status) const;

private:
    std::atomic<uint64_t> leader_ = {0};
    std::atomic<uint64_t> term_ = {0};
    std::vector<Peer> peers_;
    RaftStatus status_;
    mutable jimkv::shared_mutex mu_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
