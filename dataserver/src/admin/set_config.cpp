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

#include "admin_server.h"

#include "common/logger.h"

namespace jim {
namespace ds {
namespace admin {

using namespace dspb;

using ConfigSetFunc = std::function<Status(server::ContextServer*, const std::string&)>;
using ConfigSetFuncMap = std::map<std::string, ConfigSetFunc>;

static Status setLogLevel(server::ContextServer* ctx, const std::string& value) {
    LoggerSetLevel(value);
    return Status::OK();
}

#define SET_RANGE_SIZE(opt) \
    {"range."#opt, [](server::ContextServer *ctx, const std::string& value) { \
        (void)ctx; \
        uint64_t new_value = 0; \
        try { \
            new_value = std::stoull(value); \
        } catch (std::exception &e) { \
            return Status(Status::kInvalidArgument, "range "#opt, value); \
        } \
        ds_config.range_config.opt = new_value; \
        return Status::OK(); \
    }}

#define SET_RAFT_OPTIONS(opt) \
    {"raft."#opt, [](server::ContextServer *ctx, const std::string& value) { \
        auto rs = ctx->raft_server; \
        if (rs == nullptr) { \
            return Status(Status::kUnknown, "get raft server", ""); \
        } \
        auto s = rs->SetOptions({{#opt, value}}); \
        if (!s.ok()) { \
            return Status(Status::kIOError, "set raft options", s.ToString()); \
        } else { \
            return Status::OK(); \
        } \
    }}


static const ConfigSetFuncMap cfg_set_funcs = {
    // log level
    {"log.level",        setLogLevel},
    {"log.log_level",    setLogLevel},

    // range split
    SET_RANGE_SIZE(check_size),
    SET_RANGE_SIZE(split_size),
    SET_RANGE_SIZE(max_size),

    SET_RAFT_OPTIONS(electable),
};

Status AdminServer::setConfig(const SetConfigRequest& req, SetConfigResponse* resp) {
    for (auto &cfg: req.configs()) {
        auto it = cfg_set_funcs.find(cfg.key().section() + "." + cfg.key().name());
        if (it == cfg_set_funcs.end()) {
            return Status(Status::kNotSupported, "set config",
                    cfg.key().section() + "." + cfg.key().name());
        }
        auto s = (it->second)(context_, cfg.value());
        if (!s.ok()) {
            FLOG_WARN("[Admin] config {}.{} set to {} failed: {}",
                    cfg.key().section(), cfg.key().name(), cfg.value(), s.ToString());
            return s;
        }
        FLOG_INFO("[Admin] config {}.{} set to {}.",
                cfg.key().section(), cfg.key().name(), cfg.value());
    }
    return Status::OK();
}

} // namespace admin
} // namespace ds
} // namespace jim
