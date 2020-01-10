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

#include "masstree_options.h"

#include <sstream>

#include "common/logger.h"
#include "common/config_util.h"

namespace jimkv {

std::string CheckpointOptions::ToString() const {
    std::ostringstream ss;
    ss << "{ disabled: " << (disabled ? "true" : "false") << ", threshold: " << threshold_bytes;
    ss << ", threads: " << work_threads << ", checksum: " << (checksum ? "true" : "false");
    ss << ", max-history: " << max_history << " }";
    return ss.str();
}

std::string MasstreeOptions::ToString() const {
    std::ostringstream ss;
    ss << "{ ruc-interval: " << rcu_interval_ms << ", path: " << data_path;
    ss << ", wal-disabled: " << (wal_disabled ? "true" : "false");
    ss << ", checkpoint: " << checkpoint_opt.ToString() << " }";
    return ss.str();
}

bool MasstreeOptions::Load(const INIReader& reader, const std::string& base_data_path) {
    const char *section = "masstree";

    data_path = reader.Get(section, "path", "");
    if (!validateDataPath(base_data_path, kMasstreePathSuffix, data_path)) {
        return false;
    }

    rcu_interval_ms = reader.GetInteger(section, "rcu_interval", rcu_interval_ms);

    wal_disabled = reader.GetBoolean(section, "wal_disabled", false);
    if (wal_disabled) {
        FLOG_WARN("masstree wal is disabled");
    }

    checkpoint_opt.disabled = reader.GetBoolean(section, "checkpoint_disabled", false);
    if (checkpoint_opt.disabled) {
        FLOG_WARN("masstree checkpoint is disabled");
    }

    bool ret = false;
    std::tie(checkpoint_opt.threshold_bytes, ret) =
            loadNeBytesValue(reader, section, "checkpoint_threshold", 64*1024*1024);
    if (!ret) {
        return false;
    }

    checkpoint_opt.work_threads = reader.GetInteger(section, "checkpoint_threads", 4);
    checkpoint_opt.checksum = reader.GetBoolean(section, "checkpoint_checksum", true);
    checkpoint_opt.max_history = reader.GetInteger(section, "checkpoint_max_history", 1);

    return true;
}

} // namespace jimkv
