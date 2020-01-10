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

#include "bulletin_board.h"

namespace jimkv {
namespace raft {
namespace impl {

using jimkv::shared_lock;
using jimkv::shared_mutex;
using std::unique_lock;

void BulletinBoard::PublishLeaderTerm(uint64_t leader, uint64_t term) {
    unique_lock<shared_mutex> lock(mu_);

    leader_ = leader;
    term_ = term;
}

void BulletinBoard::PublishPeers(std::vector<Peer>&& peers) {
    unique_lock<shared_mutex> lock(mu_);

    peers_ = std::move(peers);
}

void BulletinBoard::PublishStatus(RaftStatus&& status) {
    unique_lock<shared_mutex> lock(mu_);

    status_ = std::move(status);
}

void BulletinBoard::LeaderTerm(uint64_t* leader, uint64_t* term) const {
    shared_lock<shared_mutex> lock(mu_);

    *leader = leader_;
    *term = term_;
}

void BulletinBoard::Peers(std::vector<Peer>* peers) const {
    shared_lock<shared_mutex> lock(mu_);

    *peers = peers_;
}

void BulletinBoard::Status(RaftStatus* status) const {
    shared_lock<shared_mutex> lock(mu_);

    *status = status_;
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
