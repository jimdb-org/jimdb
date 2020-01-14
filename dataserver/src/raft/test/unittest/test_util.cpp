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

#include "test_util.h"

#include "base/util.h"

namespace jim {
namespace raft {
namespace impl {
namespace testutil {

using jim::randomInt;
using jim::randomString;

Peer RandomPeer() {
    Peer p;
    p.type = (randomInt() % 2 == 0 ? jim::raft::PeerType::kLearner
                                   : jim::raft::PeerType::kNormal);
    p.node_id = randomInt();
    p.peer_id = randomInt();
    return p;
}

bool EqualPeer(const Peer& p1, const Peer& p2) {
    return p1.type == p2.type && p1.node_id == p2.node_id && p1.peer_id == p2.peer_id;
}

Status Equal(const pb::HardState& lh, const pb::HardState& rh) {
    if (lh.term() != rh.term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh.term()) + " != " + std::to_string(rh.term()));
    }
    if (lh.vote() != rh.vote()) {
        return Status(Status::kCorruption, "vote",
                      std::to_string(lh.vote()) + " != " + std::to_string(rh.vote()));
    }
    if (lh.commit() != rh.commit()) {
        return Status(Status::kCorruption, "commit",
                      std::to_string(lh.commit()) + " != " + std::to_string(rh.commit()));
    }
    return Status::OK();
}

Status Equal(const pb::TruncateMeta& lh, const pb::TruncateMeta& rh) {
    if (lh.term() != rh.term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh.term()) + " != " + std::to_string(rh.term()));
    }
    if (lh.index() != rh.index()) {
        return Status(Status::kCorruption, "index",
                      std::to_string(lh.index()) + " != " + std::to_string(rh.index()));
    }
    return Status::OK();
}

EntryPtr RandomEntry(uint64_t index, int data_size) {
    EntryPtr e(new jim::raft::impl::pb::Entry);
    e->set_index(index);
    e->set_term(randomInt());
    e->set_type((randomInt() % 2 == 0) ? jim::raft::impl::pb::ENTRY_NORMAL
                                       : jim::raft::impl::pb::ENTRY_CONF_CHANGE);
    e->set_data(randomString(data_size));
    return e;
}

void RandomEntries(uint64_t lo, uint64_t hi, int data_size,
                   std::vector<EntryPtr>* entries) {
    for (uint64_t i = lo; i < hi; ++i) {
        entries->push_back(RandomEntry(i, data_size));
    }
}

Status Equal(const EntryPtr& lh, const EntryPtr& rh) {
    if (lh->index() != rh->index()) {
        return Status(Status::kCorruption, "index",
                      std::to_string(lh->index()) + " != " + std::to_string(rh->index()));
    }
    if (lh->term() != rh->term()) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh->term()) + " != " + std::to_string(rh->term()));
    }
    if (lh->type() != rh->type()) {
        return Status(Status::kCorruption, "type",
                      std::to_string(lh->type()) + " != " + std::to_string(rh->type()));
    }
    if (lh->data() != rh->data()) {
        return Status(Status::kCorruption, "data", lh->data() + " != " + rh->data());
    }

    return Status::OK();
}

Status Equal(const std::vector<EntryPtr>& lh, const std::vector<EntryPtr>& rh) {
    if (lh.size() != rh.size()) {
        return Status(Status::kCorruption, "entries size",
                      std::to_string(lh.size()) + " != " + std::to_string(rh.size()));
    }
    for (size_t i = 0; i < lh.size(); ++i) {
        const auto& le = lh[i];
        const auto& re = rh[i];
        auto s = Equal(le, re);
        if (!s.ok()) {
            return Status(Status::kCorruption,
                          std::string("at index ") + std::to_string(i), s.ToString());
        }
    }
    return Status::OK();
}

SnapContext randSnapContext() {
    SnapContext ctx;
    ctx.id = static_cast<uint64_t>(randomInt());
    ctx.term = static_cast<uint64_t>(randomInt());
    ctx.uuid = static_cast<uint64_t>(randomInt());
    ctx.from = static_cast<uint64_t>(randomInt());
    ctx.to = static_cast<uint64_t>(randomInt());
    return ctx;
};

Status Equal(const SnapContext& lh, const SnapContext& rh) {
    if (lh.from != rh.from) {
        return Status(Status::kCorruption, "from",
                      std::to_string(lh.from) + " != " + std::to_string(rh.from));
    }
    if (lh.id != rh.id) {
        return Status(Status::kCorruption, "id",
                      std::to_string(lh.id) + " != " + std::to_string(rh.id));
    }
    if (lh.term != rh.term) {
        return Status(Status::kCorruption, "term",
                      std::to_string(lh.term) + " != " + std::to_string(rh.term));
    }
    if (lh.to != rh.to) {
        return Status(Status::kCorruption, "to",
                      std::to_string(lh.to) + " != " + std::to_string(rh.to));
    }
    if (lh.uuid != rh.uuid) {
        return Status(Status::kCorruption, "uuid",
                      std::to_string(lh.uuid) + " != " + std::to_string(rh.uuid));
    }
    return Status::OK();
}

} /* namespace testutil */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
