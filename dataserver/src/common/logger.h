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

#include <stdarg.h>
#include "spdlog/spdlog.h"

namespace jim {

struct LoggerConfig {
    std::string path;
    std::string name;
    std::string level;
    size_t max_size = 1024 * 1024 * 1024;
    size_t max_files = 10;
    size_t flush_interval = 5; // in seconds
};

bool LoggerSetLevel(const std::string &level);
bool LoggerInit(const LoggerConfig& config);

} // namespace jim


#define FLOG_TRACE(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::trace, fmt, ##__VA_ARGS__); \
    }

#define FLOG_DEBUG(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::debug)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::debug, fmt, ##__VA_ARGS__); \
    }

#define FLOG_INFO(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::info)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::info, fmt, ##__VA_ARGS__); \
    }

#define FLOG_WARN(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::warn)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::warn, fmt, ##__VA_ARGS__); \
    }

#define FLOG_ERROR(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::err)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::err, fmt, ##__VA_ARGS__); \
    }

#define FLOG_CRIT(fmt, ...) \
    if (spdlog::default_logger_raw()->should_log(spdlog::level::critical)) { \
        spdlog::log(spdlog::source_loc(__FNAME__, __LINE__, __FUNCTION__), spdlog::level::critical, fmt, ##__VA_ARGS__); \
    }
