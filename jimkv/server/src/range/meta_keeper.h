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

#include "base/status.h"
#include "base/timer.h"
#include "basepb/basepb.pb.h"

namespace jimkv {
namespace ds {
namespace range {

// MetaKeeper:
// manager query and update operation to metapb::Range
class MetaKeeper {
public:
    MetaKeeper(const basepb::Range& m, TimerQueue* timer_queue);
    MetaKeeper(basepb::Range&& m, TimerQueue* timer_queue);
    ~MetaKeeper();

    MetaKeeper(const MetaKeeper&) = delete;
    MetaKeeper& operator=(const MetaKeeper&) = delete;

    basepb::Range Get() const;
    void Get(basepb::Range *out) const;
    void Set(const basepb::Range& to);
    void Set(basepb::Range&& to);

    uint64_t GetTableID() const;
    std::string GetStartKey() const;
    std::string GetEndKey() const;

    void GetEpoch(basepb::RangeEpoch* epoch) const;
    bool EpochIsEqual(const basepb::RangeEpoch& epoch) const;
    uint64_t GetConfVer() const;
    uint64_t GetVersion() const;

    Status AddPeer(const basepb::Peer& peer, uint64_t verify_conf_ver);
    Status DelPeer(const basepb::Peer& peer, uint64_t verify_conf_ver);
    Status PromotePeer(uint64_t node_id, uint64_t peer_id);

    std::vector<basepb::Peer> GetAllPeers() const;
    bool FindPeer(uint64_t peer_id, basepb::Peer* peer = nullptr) const;
    bool FindPeerByNodeID(uint64_t node_id, basepb::Peer* peer = nullptr) const;

    Status CheckSplit(const std::string& end_key, uint64_t version) const;
    void Split(const std::string& end_key, uint64_t new_version);

    std::string ToString() const;

    Status verifyConfVer(uint64_t conf_ver) const;
    Status verifyVersion(uint64_t version) const;

private:
    basepb::Range* get() const;
    void set(std::unique_ptr<basepb::Range> new_meta);

private:
    TimerQueue* timer_queue_ = nullptr;
    std::atomic<basepb::Range *> meta_ptr_ = { nullptr };
};

}  // namespace range
}  // namespace ds
}  // namespace jimkv
