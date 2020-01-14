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

#include "inih/INIReader.h"

#include "common/logger.h"

namespace jim {

static const char *kMetaPathSuffix = "meta";
static const char *kRaftPathSuffix = "raft";
static const char *kMasstreePathSuffix = "massdb";
static const char *kRocksdbPathSuffix = "rocksdb";

std::pair<uint64_t, bool> loadNeBytesValue(const INIReader& reader, const char *section,
        const char *item, size_t default_value);

std::pair<size_t, bool> loadThreadNum(const INIReader& reader, const char *section,
        const char *item, int default_value);

std::pair<uint16_t, bool> loadUInt16(const INIReader& reader, const char *section, const char *item);

template <class T>
static std::pair<T, bool> loadIntegerAtLeast(const INIReader& reader, const char *section,
                                             const char *item, T default_value, T atleast_value) {
    auto value = reader.GetInteger(section, item, default_value);
    if (static_cast<T>(value) < atleast_value) {
        FLOG_CRIT("[Config] invalid config {}.{}: {}, at least: {}", section, item, value, atleast_value);
        return {0, false};
    }
    return {static_cast<T>(value), true};
}

// if type path is empty, use {base_path}/{type_name} as default path
bool validateDataPath(const std::string& base_path, const std::string& type_name, std::string& type_path);

} // namespace jim
