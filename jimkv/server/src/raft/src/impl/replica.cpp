// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include "replica.h"

#include <sstream>
#include "raft_exception.h"

namespace jimkv {
namespace raft {
namespace impl {

Inflight::Inflight(int max) : capacity_(max), buffer_(max) {}

void Inflight::add(uint64_t index) {
    if (full()) {
        throw RaftException("inflight.add cannot add into a full inflights.");
    }

    int idx = (start_ + count_) % capacity_;
    buffer_[idx] = index;
    ++count_;
}

void Inflight::freeTo(uint64_t index) {
    if (0 == count_ || index < buffer_[start_]) {
        return;
    }
    int i = 0, idx = start_;
    for (; i < count_; ++i) {
        if (index < buffer_[idx]) {
            break;
        }
        ++idx;
        idx %= capacity_;
    }
    count_ -= i;
    start_ = idx;
}

void Inflight::freeFirstOne() { freeTo(buffer_[start_]); }

bool Inflight::full() const { return count_ == capacity_; }

void Inflight::reset() {
    count_ = 0;
    start_ = 0;
}

Replica::Replica(const Peer& peer, int max_inflight)
    : peer_(peer), inflight_(max_inflight) {}

void Replica::resetState(ReplicaState state) {
    paused_ = false;
    pendingSnap_ = 0;
    state_ = state;
    inflight_.reset();
}

void Replica::becomeProbe() {
    if (state_ == ReplicaState::kSnapshot) {
        uint64_t pendingSnap = pendingSnap_;
        resetState(ReplicaState::kProbe);
        next_ = std::max(match_ + 1, pendingSnap + 1);
    } else {
        resetState(ReplicaState::kProbe);
        next_ = match_ + 1;
    }
}

void Replica::becomeReplicate() {
    resetState(ReplicaState::kReplicate);
    next_ = match_ + 1;
}

void Replica::becomeSnapshot(uint64_t index) {
    resetState(ReplicaState::kSnapshot);
    pendingSnap_ = index;
}

void Replica::update(uint64_t index) { next_ = index + 1; }

bool Replica::maybeUpdate(uint64_t index, uint64_t commit) {
    bool updated = false;
    if (committed_ < commit) {
        committed_ = commit;
    }
    if (match_ < index) {
        match_ = index;
        updated = true;
        resume();
    }
    uint64_t next = index + 1;
    if (next_ < next) {
        next_ = next;
    }
    return updated;
}

bool Replica::maybeDecrTo(uint64_t rejected, uint64_t last, uint64_t commit) {
    if (state_ == ReplicaState::kReplicate) {
        if (committed_ < commit) {
            committed_ = commit;
        }
        if (rejected <= match_) {
            return false;
        }
        next_ = match_ + 1;
        return true;
    }
    if (next_ - 1 != rejected) {
        return false;
    }
    next_ = std::min(rejected, last + 1);
    if (next_ < 1) {
        next_ = 1;
    }
    committed_ = commit;
    resume();
    return true;
}

void Replica::snapshotFailure() { pendingSnap_ = 0; }

bool Replica::needSnapshotAbort() {
    return state_ == ReplicaState::kSnapshot && match_ >= pendingSnap_;
}

void Replica::pause() { paused_ = true; }

void Replica::resume() { paused_ = false; }

bool Replica::isPaused() const {
    switch (state_) {
        case ReplicaState::kProbe:
            return paused_;
        case ReplicaState::kSnapshot:
            return true;
        default:
            return inflight_.full();
    }
}

bool Replica::alive() const {
    return alive_;
}

void Replica::setAlive(bool alive) {
    alive_ = alive;
}

std::string Replica::ToString() const {
    std::ostringstream ss;
    ss << "next=" << next_ << ", match=" << match_ << ", commit=" << committed_
       << ", state=" << ReplicateStateName(state_)
       << ", pendingSnapshot=" << pendingSnap_;
    return ss.str();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
