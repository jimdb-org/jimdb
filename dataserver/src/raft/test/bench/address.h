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

#include <map>
#include <vector>
#include "raft/node_resolver.h"

namespace jim {
namespace raft {
namespace bench {

class NodeAddress : public NodeResolver {
public:
    explicit NodeAddress(int n);
    ~NodeAddress();

    std::string GetNodeAddress(uint64_t node_id) override;

    uint16_t GetListenPort(uint64_t port) const;

    void GetAllNodes(std::vector<uint64_t>* nodes) const;

private:
    std::map<uint64_t, uint16_t> ports_;
};

} /* namespace bench */
} /* namespace raft */
} /* namespace jim */
