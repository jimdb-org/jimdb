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

#include "config_util.h"

#include "base/fs_util.h"

namespace jim {

std::pair<uint64_t, bool> loadNeBytesValue(const INIReader& reader, const char *section,
                                                  const char *item, size_t default_value) {
    auto value_str = reader.Get(section, item, "");
    if (value_str.empty()) {
        return {default_value, true};
    }

    int64_t value = 0;
    if (ParseBytesValue(value_str.c_str(), &value) < 0) {
        FLOG_CRIT("[Config] could not parse invalid {}.{}: {}", section, item, value_str);
        return {0, false};
    } else {
        return {static_cast<uint64_t>(value), true};
    }
}

std::pair<size_t, bool> loadThreadNum(const INIReader& reader, const char *section,
                                      const char *item, int default_value) {
    static const int kMaxThreadNum = 255;
    auto val = reader.GetInteger(section, item, default_value);
    if (val < 1 || val > kMaxThreadNum) {
        FLOG_CRIT("[Config] invalid config {}.{}: {}", section, item, val);
        return {0, false};
    } else {
        return {static_cast<size_t>(val), true};
    }
}

std::pair<uint16_t, bool> loadUInt16(const INIReader& reader, const char *section, const char *item) {
    auto val = reader.GetInteger(section, item, 0);
    if (val < 1 || val > static_cast<long>(std::numeric_limits<uint16_t>::max())) {
        FLOG_CRIT("[Config] invalid config {}.{}: {}", section, item, val);
        return {0, false};
    } else {
        return {static_cast<uint16_t>(val), true};
    }
}

bool validateDataPath(const std::string& base_path, const std::string& type_name, std::string& type_path) {
    if (!type_path.empty()) {
        return true;
    }
    if (!base_path.empty()) {
        type_path = JoinFilePath({base_path, type_name});
        return true;
    } else {
        FLOG_CRIT("invalid {} data path: {}, base: {}", type_name, type_path, base_path);
        return false;
    }
}

} // namespace jim

