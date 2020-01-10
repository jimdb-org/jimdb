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

#include "meta_keeper.h"

#include <sstream>

#include "base/util.h"

namespace jimkv {
namespace ds {
namespace range {

static const int kMetaDelayFreeMs = 10 * 1000;

class DelayFreeTimer : public Timer {
public:
    explicit DelayFreeTimer(basepb::Range* ptr) : ptr_(ptr) {}

    void OnTimeout() override {
        delete ptr_;
    }

private:
    basepb::Range* ptr_ = nullptr;
};

MetaKeeper::MetaKeeper(const basepb::Range& m, TimerQueue* timer_queue) :
    timer_queue_(timer_queue),
    meta_ptr_(new basepb::Range(m)) {
}

MetaKeeper::MetaKeeper(basepb::Range&& m, TimerQueue* timer_queue) :
    timer_queue_(timer_queue),
    meta_ptr_(new basepb::Range(std::move(m))) {
}

MetaKeeper::~MetaKeeper() {
    delete meta_ptr_.load();
}

basepb::Range* MetaKeeper::get() const {
    return meta_ptr_.load();
}

void MetaKeeper::set(std::unique_ptr<basepb::Range> new_meta) {
    auto old_meta = get();
    assert(new_meta);

    // set to new
    meta_ptr_.store(new_meta.release());

    // delay free old one
    if (timer_queue_ != nullptr) {
        auto timer = std::make_shared<DelayFreeTimer>(old_meta);
        timer_queue_->Push(timer, kMetaDelayFreeMs);
    }
}

basepb::Range MetaKeeper::Get() const {
    return *get();
}

void MetaKeeper::Get(basepb::Range *out) const {
    out->CopyFrom(*get());
}

void MetaKeeper::Set(const basepb::Range& to) {
    set(std::unique_ptr<basepb::Range>(new basepb::Range(to)));
}

void MetaKeeper::Set(basepb::Range&& to) {
    set(std::unique_ptr<basepb::Range>(new basepb::Range(std::move(to))));
}

void MetaKeeper::GetEpoch(basepb::RangeEpoch* epoch) const {
    epoch->CopyFrom(get()->range_epoch());
}

bool MetaKeeper::EpochIsEqual(const basepb::RangeEpoch& epoch) const {
    auto meta = get();
    return meta->range_epoch().version() == epoch.version() &&
           meta->range_epoch().conf_ver() == epoch.conf_ver();
}

uint64_t MetaKeeper::GetConfVer() const {
    return get()->range_epoch().conf_ver();
}

uint64_t MetaKeeper::GetVersion() const {
    return get()->range_epoch().version();
}

uint64_t MetaKeeper::GetTableID() const {
    return get()->table_id();
}

std::string MetaKeeper::GetStartKey() const {
    return get()->start_key();
}

std::string MetaKeeper::GetEndKey() const {
    return get()->end_key();
}

Status MetaKeeper::verifyConfVer(uint64_t conf_ver) const {
    uint64_t current = get()->range_epoch().conf_ver();
    if (conf_ver == current) {
        return Status::OK();
    }

    std::ostringstream ss;
    ss << "current is: " << current;
    ss << ", to verify is: " << conf_ver;

    if (conf_ver < current) {
        return Status(Status::kStaleEpoch, "conf ver", ss.str());
    } else {
        assert(conf_ver != current);
        return Status(Status::kInvalidArgument, "conf ver", ss.str());
    }
}

Status MetaKeeper::AddPeer(const basepb::Peer& peer, uint64_t verify_conf_ver) {
    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    auto old_meta = get();
    // check peer already exist
    for (const auto& old : old_meta->peers()) {
        if (old.node_id() == peer.node_id() || old.id() == peer.id()) {
            return Status(Status::kExisted,
                    std::string("old is ") + old.ShortDebugString(),
                    std::string("to add is ") + peer.ShortDebugString());
        }
    }

    // copy and write
    std::unique_ptr<basepb::Range> new_meta(new basepb::Range(*old_meta));
    new_meta->add_peers()->CopyFrom(peer);
    new_meta->mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);
    set(std::move(new_meta));

    return Status::OK();
}

Status MetaKeeper::DelPeer(const basepb::Peer& peer, uint64_t verify_conf_ver) {
    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    // copy and write
    // find and delete
    std::unique_ptr<basepb::Range> new_meta(new basepb::Range(*get()));
    for (auto it = new_meta->peers().cbegin(); it != new_meta->peers().cend(); ++it) {
        auto same_node = it->node_id() == peer.node_id();
        auto same_id = it->id() == peer.id();
        if (same_node && same_id) {
            new_meta->mutable_peers()->erase(it);
            new_meta->mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);
            set(std::move(new_meta));
            return Status::OK();
        }
    }
    return Status(Status::kNotFound);
}

Status MetaKeeper::PromotePeer(uint64_t node_id, uint64_t peer_id) {
    std::unique_ptr<basepb::Range> new_meta(new basepb::Range(*get()));
    for (int i = 0; i < new_meta->peers_size(); ++i) {
        auto mp = new_meta->mutable_peers(i);
        if (mp->id() == peer_id && mp->node_id() == node_id) {
            mp->set_type(basepb::PeerType_Normal);
            set(std::move(new_meta));
            return Status::OK();
        }
    }

    return Status(Status::kNotFound);
}

std::vector<basepb::Peer> MetaKeeper::GetAllPeers() const {
    auto meta = get();
    std::vector<basepb::Peer> peers;
    for (const auto& p : meta->peers()) {
        peers.emplace_back(p);
    }
    return peers;
}

bool MetaKeeper::FindPeer(uint64_t peer_id, basepb::Peer* peer) const {
    auto meta = get();
    for (const auto& p: meta->peers()) {
        if (p.id() == peer_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

bool MetaKeeper::FindPeerByNodeID(uint64_t node_id, basepb::Peer* peer) const {
    auto meta = get();
    for (const auto& p: meta->peers()) {
        if (p.node_id() == node_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

Status MetaKeeper::verifyVersion(uint64_t version) const {
    uint64_t current = get()->range_epoch().version();
    if (version == current) {
        return Status::OK();
    }

    std::ostringstream ss;
    ss << "current is: " << current;
    ss << ", to verify is: " << version;

    if (version < current) {
        return Status(Status::kStaleEpoch, "conf ver", ss.str());
    } else {
        assert(version != current);
        return Status(Status::kInvalidArgument, "conf ver", ss.str());
    }
}

Status MetaKeeper::CheckSplit(const std::string& end_key, uint64_t version) const {
    auto meta = get();
    // check end key valid
    if (end_key >= meta->end_key() || end_key <= meta->start_key()) {
        std::ostringstream ss;
        ss << EncodeToHex(end_key) << " out of bound [ ";
        ss << EncodeToHex(meta->start_key()) << " - ";
        ss << EncodeToHex(meta->end_key()) << "]";
        return Status(Status::kOutOfBound, "split end key", ss.str());
    }
    // check version
    return verifyVersion(version);
}

void MetaKeeper::Split(const std::string& end_key, uint64_t new_version) {
    std::unique_ptr<basepb::Range> new_meta(new basepb::Range(*get()));
    new_meta->set_end_key(end_key);
    new_meta->mutable_range_epoch()->set_version(new_version);
    set(std::move(new_meta));
}

std::string MetaKeeper::ToString() const {
    return get()->ShortDebugString();
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
