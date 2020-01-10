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


#include "jimkv_route.h"

#include "util.h"
#include "jimkv_log.h"

namespace jimkv {
namespace sdk {

RangePtr JimRoute::GetRange(const std::string& key) {
    //gcc 5.2 c++ 14 ....(T_T)
    JimScope sk(key, key);
    shared_lock<shared_mutex> lock(map_lock_);
    auto it = range_map_.find(sk);
    if (it != range_map_.end()) {
        return it->second;
    }
    return nullptr;
}

RangePtr JimRoute::GetRange(uint64_t range_id) {
    shared_lock<shared_mutex> lock(map_lock_);
    auto it = id_map_.find(range_id);
    if (it != id_map_.end()) {
        return it->second;
    }

    return nullptr;
}

void JimRoute::Insert(const basepb::Range& range) {
    JimScope key(range.start_key(), range.end_key());
    auto rng = GetRange(range.id());

    std::lock_guard<shared_mutex> lock(map_lock_);
    if (rng != nullptr) {
        auto &lhs = range.range_epoch();
        auto &rhs = rng->range_epoch();
        if (lhs.conf_ver() == rhs.conf_ver() && lhs.version() == rhs.version()) {
            return;
        }

        CBLOG_INFO("remove route [%s-%s)", EncodeToOct(rng->start_key()).c_str(),
            EncodeToOct(rng->end_key()).c_str());

        range_map_.erase(key);
        id_map_.erase(rng->id());
    }
    CBLOG_INFO("insert route [%s-%s)", EncodeToOct(range.start_key()).c_str(),
        EncodeToOct(range.end_key()).c_str());

    auto new_rng = std::make_shared<basepb::Range>(std::move(range));
    id_map_[new_rng->id()] = new_rng;
    range_map_[key] = new_rng;
}

bool JimRoute::Update(uint64_t range_id, uint64_t leader,
    const basepb::RangeEpoch& epoch)
{
    auto range = GetRange(range_id);
    if (range != nullptr) {
        range->set_leader(leader);
        range->mutable_range_epoch()->set_conf_ver(epoch.conf_ver());
        range->mutable_range_epoch()->set_version(epoch.version());
        return true;
    }

    CBLOG_WARN("update route fail, not found (id):%" PRIu64 , range_id);
    return false;
}

void JimRoute::Erase(std::string& key) {
    auto rng = GetRange(key);
    if (rng != nullptr) {
        CBLOG_INFO("remove route [%s-%s)", EncodeToOct(rng->start_key()).c_str(),
            EncodeToOct(rng->end_key()).c_str());

        JimScope sk(key, key);
        std::lock_guard<shared_mutex> lock(map_lock_);
        range_map_.erase(sk);
        id_map_.erase(rng->id());
    }
}

void JimRoute::Erase(uint64_t range_id) {
    auto rng = GetRange(range_id);
    if (rng != nullptr) {
        CBLOG_INFO("remove route [%s-%s)", EncodeToOct(rng->start_key()).c_str(),
            EncodeToOct(rng->end_key()).c_str());

        JimScope sk(rng->start_key(),rng->start_key());
        std::lock_guard<shared_mutex> lock(map_lock_);
        range_map_.erase(sk);
        id_map_.erase(range_id);
    }
}


} // namespace sdk
} // namespace jimkv

