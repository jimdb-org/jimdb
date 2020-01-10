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

_Pragma ("once");

#include <set>
#include <map>
#include <string>
#include <memory>
#include <mutex>

#include "basepb/basepb.pb.h"

#include "jimkv_scope.h"
#include "base/shared_mutex.h"

namespace jimkv {
namespace sdk {

using RangePtr = std::shared_ptr<basepb::Range>;
//using RangeMap = std::map<JimScope, RangePtr, std::less<>>;
using RangeMap = std::map<JimScope, RangePtr>;

class JimRoute final {
public:
    JimRoute() = default;
    ~JimRoute() = default;

    JimRoute(const JimRoute &) = delete;
    JimRoute& operator=(const JimRoute &) = delete;

    RangePtr GetRange(const std::string& key);
    RangePtr GetRange(uint64_t range_id);
    bool Update(uint64_t range_id, uint64_t leader, const basepb::RangeEpoch& epoch);

    void Insert(const basepb::Range& range);

    void Erase(std::string& key);
    void Erase(uint64_t range_id);

    //const RangeMap& ranges() {return range_map_;}

private:

    RangeMap range_map_;
    std::map<uint64_t, RangePtr> id_map_;
    shared_mutex map_lock_;
};

using RoutePtr = std::shared_ptr<JimRoute>;

} // namespace sdk
} // namespace jimkv

