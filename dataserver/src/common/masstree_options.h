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

#include <string>

#include "inih/INIReader.h"

namespace jim {

struct CheckpointOptions {
    bool disabled = false;
    uint64_t threshold_bytes = 64 * 1024 * 1024; // default 64MB
    uint64_t work_threads = 4;
    bool checksum = true;
    size_t max_history = 1;

    std::string ToString() const;
};

struct MasstreeOptions {
    std::string data_path;   // checkpoint path
    size_t rcu_interval_ms = 1000;
    bool wal_disabled = false;
    CheckpointOptions checkpoint_opt;

    bool Load(const INIReader& reader, const std::string& base_data_path);
    std::string ToString() const;
};

} // namespace jim
