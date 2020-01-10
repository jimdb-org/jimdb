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

#include "admin_server.h"

#include <map>
#include <string>
#include <functional>

#include "common/server_config.h"
#include "common/logger.h"

namespace jimkv {
namespace ds {
namespace admin {

using namespace dspb;

using ConfigGeterMap = std::map<std::string, std::function<std::string()>> ;

#define ADD_CFG_GETTER(section, name) \
    {#section"."#name, [] { return std::to_string(ds_config.section##_config.name); }}

#define ADD_CFG_GETTER_STR(section, name) \
    {#section"."#name, [] { return std::string(ds_config.section##_config.name); }}

static const ConfigGeterMap cfg_getters = {
        // log
        {"log.level", []{return ds_config.logger_config.level;}},

        // range
        ADD_CFG_GETTER(range, recover_skip_fail),
        ADD_CFG_GETTER(range, recover_concurrency),
        ADD_CFG_GETTER(range, check_size),
        ADD_CFG_GETTER(range, split_size),
        ADD_CFG_GETTER(range, max_size),
        ADD_CFG_GETTER(range, worker_threads),

        // raft
        ADD_CFG_GETTER(raft, port),
        ADD_CFG_GETTER_STR(raft, log_path),
        ADD_CFG_GETTER(raft, log_file_size),
        ADD_CFG_GETTER(raft, max_log_files),
        ADD_CFG_GETTER(raft, allow_log_corrupt),
        ADD_CFG_GETTER(raft, consensus_threads),
        ADD_CFG_GETTER(raft, consensus_queue),
        ADD_CFG_GETTER(raft, transport_send_threads),
        ADD_CFG_GETTER(raft, transport_recv_threads),
        ADD_CFG_GETTER(raft, tick_interval_ms),
        ADD_CFG_GETTER(raft, max_msg_size),

        // worker
        {"worker.schedule_worker", [] { return std::to_string(ds_config.worker_config.schedule_worker_num); }},
        {"worker.slow_worker", [] { return std::to_string(ds_config.worker_config.slow_worker_num); }},

        // manager
        ADD_CFG_GETTER(manager, port),

        // heartbeat
        {"cluster.cluster.id", []{ return std::to_string(ds_config.cluster_config.cluster_id); }},
        {"cluster.node_heartbeat_interval", []{ return std::to_string(ds_config.cluster_config.node_interval_secs ); }},
        {"cluster.range_heartbeat_interval", []{ return std::to_string(ds_config.cluster_config.range_interval_secs); }},
        {"cluster.master_host", []{
            std::string result;
            for (const auto &host : ds_config.cluster_config.master_host) {
                if (!host.empty()) {
                    result += host;
                    result += ",";
                }
            }
            if (!result.empty()) result.pop_back();
            return result;
        }},
};

static bool getConfigValue(const ConfigKey& key, std::string* value) {
    FLOG_DEBUG("[Admin] get config: {}.{}", key.section(), key.name());

    auto it = cfg_getters.find(key.section() + "." + key.name());
    if (it != cfg_getters.end()) {
        *value = (it->second)();
        return true;
    } else {
        return false;
    };
}

Status AdminServer::getConfig(const dspb::GetConfigRequest& req, dspb::GetConfigResponse* resp) {
    for (const auto& key: req.key()) {
        auto cfg = resp->add_configs();
        cfg->mutable_key()->CopyFrom(key);
        if(!getConfigValue(key, cfg->mutable_value())) {
            return Status(Status::kNotFound, "config key", key.section() + "." + key.name());
        }
    }
    return Status::OK();
}

} // namespace admin
} // namespace ds
} // namespace jimkv
