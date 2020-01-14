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

#include "mutable_options.h"

#include "common/logger.h"

namespace jim {
namespace raft {
namespace impl {

static bool parseBoolean(const std::string& val, bool& res) {
    // tolower
    auto lower_val = val;
    std::transform(lower_val.begin(), lower_val.end(), lower_val.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    if (lower_val == "0" || lower_val == "false") {
        res = false;
        return true;
    } else if (lower_val == "1" || lower_val == "true") {
        res = true;
        return true;
    } else {
        return false;
    }
}

Status ServerMutableOptions::Set(const std::map<std::string, std::string>& opt_map) {
    for (const auto& p: opt_map) {
        if (p.first == "electable") {
            return setElectable(p.second);
        } else {
            return Status(Status::kNotSupported, "set raft server option", p.first);
        }
    }
    return Status::OK();
}

Status ServerMutableOptions::setElectable(const std::string& val) {
    bool new_cfg = false;
    if (parseBoolean(val, new_cfg)) {
        electable_ = new_cfg;
        FLOG_WARN("[raft config] set electable to {}", new_cfg);
        return Status::OK();
    } else {
        return Status(Status::kInvalidArgument, "set raft electable options", val);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */

