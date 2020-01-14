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

#include "logger.h"
#include <iostream>

#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "base/util.h"
#include "base/fs_util.h"
#include "common/server_config.h"

namespace jim {

static std::pair<spdlog::level::level_enum, bool> parseLevel(const std::string& level) {
    spdlog::level::level_enum log_level;
    if (level == "trace" || level == "TRACE") {
        log_level = spdlog::level::trace;
    } else if (level == "debug" || level == "DEBUG") {
        log_level = spdlog::level::debug;
    } else if (level == "info" || level == "INFO") {
        log_level = spdlog::level::info;
    } else if (level == "warn" || level == "WARN" || level == "warning" || level == "WARNING") {
        log_level = spdlog::level::warn;
    } else if (level == "err" || level == "ERR" || level == "error" || level == "ERROR") {
        log_level = spdlog::level::err;
    } else if (level == "crit" || level == "CRIT" || level == "critical" || level == "CRITICAL") {
        log_level = spdlog::level::critical;
    } else {
        FLOG_CRIT("invalid log level: {}", level);
        return {log_level, false};
    }
    return {log_level, true};
}

bool LoggerSetLevel(const std::string &level) {
    auto level_ret = parseLevel(level);
    if (!level_ret.second) {
        return false;
    }
    ds_config.logger_config.level.assign(level);
    spdlog::set_level(level_ret.first);

    return true;
}

bool LoggerInit(const LoggerConfig& config) {
    if (!MakeDirAll(config.path, 0755)) {
        FLOG_CRIT("init log path failed: {}", strErrno(errno));
        return false;
    }

    std::string log_file = JoinFilePath({config.path, config.name});
    log_file += ".log";

    auto logger = spdlog::rotating_logger_mt(config.name, log_file,
            config.max_size, config.max_files);

    //auto logger = spdlog::daily_logger_mt(config.name, log_file, 0, 0);
    if (!logger) {
        FLOG_CRIT("init log file error.");
        return false;
    }
    spdlog::set_default_logger(logger);

    spdlog::set_pattern("[%Y-%m-%d %T.%e] [%l] [%t] [%@] %v");
    //spdlog::set_pattern("[%H:%M:%S.%e] [%l] [%t] [%@ %!] %v");
    spdlog::flush_every(std::chrono::seconds(config.flush_interval));
    spdlog::flush_on(spdlog::level::err);

    return LoggerSetLevel(config.level);
}

} // namespace jim
