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

#include <string>
#include <sstream>
#include <rocksdb/version.h>

#ifdef JIM_BUILD_VERSION
#include "version_gen.h"
#else
#define GIT_DESC "<Unknown>"
#define BUILD_DATE "<Unknown>"
#define BUILD_TYPE "<Unknown>"
#define BUILD_FLAGS "<Unknown>"
#endif

namespace jim {
namespace ds {
namespace server {

std::string GetGitDescribe() {
    return GIT_DESC;
}

std::string GetBuildDate() {
    return BUILD_DATE;
}

std::string GetBuildType() {
    return BUILD_TYPE;
}

std::string GetVersionInfo() {
    std::ostringstream ss;
    ss << "Git Describe:\t" << GIT_DESC << std::endl;
    ss << "Build Date:\t" << BUILD_DATE << std::endl;
    ss << "Build Type:\t" << BUILD_TYPE << std::endl;
    ss << "Build Flags:\t" << BUILD_FLAGS << std::endl;
    ss << "Rocksdb:\t" << ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH;
    return ss.str();
}

std::string GetRocksdbVersion() {
    return std::to_string(ROCKSDB_MAJOR) + "." + std::to_string(ROCKSDB_MINOR)
        + "." + std::to_string(ROCKSDB_PATCH);
}

} /* namespace server */
} /* namespace ds */
} /* namespace jim */
