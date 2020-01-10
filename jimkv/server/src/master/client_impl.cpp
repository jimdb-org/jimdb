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

#include "client_impl.h"

#include <curl/curl.h>

#include "base/util.h"
#include "common/logger.h"

namespace jimkv {
namespace ds {
namespace master {

static const std::string kProtocalSchema = "http://";
static const cpr::Header kHTTPHeader {{"Content-Type", "application/proto"}};
static const std::string kRegisterURI = "/node/register";
static const std::string kNodeHeartbeatURI = "/node/heartbeat";
static const std::string kNodeGetURI = "/node/get";
static const std::string kRangeHeartbeatURI = "/range/heartbeat";
static const std::string kAskSplitURI = "/range/ask_split";
static const size_t kMinRetryTimes = 3;
static const cpr::Timeout kRequestTimeout{5000}; // in milliseconds

thread_local cpr::Session MasterClientImpl::session_ = {};

MasterClientImpl::MasterClientImpl(uint64_t cluster_id, const std::vector<std::string> &ms_addrs) :
    cluster_id_(cluster_id),
    ms_addrs_(ms_addrs.cbegin(), ms_addrs.cend()) {
    if (ms_addrs.empty()) {
        throw std::runtime_error("invalid master server addresses(empty).");
    }
}

Status MasterClientImpl::post(const std::string &uri,
        const google::protobuf::Message& req, google::protobuf::Message &resp) {
    FLOG_DEBUG("[Master] post uri: {} request:{}", uri, req.ShortDebugString());
    Status ret;
    auto index = ++rr_counter_;
    // retry all master addrs if response not ok
    auto retry_times = std::max(kMinRetryTimes, ms_addrs_.size());
    for (size_t i = 0; i < retry_times; ++i) {
        auto ms_addr = ms_addrs_[(index + i) % ms_addrs_.size()];
        std::string url = kProtocalSchema + ms_addr + uri;
        session_.SetUrl(url);
        session_.SetTimeout(kRequestTimeout);
        session_.SetHeader(kHTTPHeader);
        cpr::Body req_body{req.SerializeAsString()};
        session_.SetBody(std::move(req_body));
        auto cpr_resp = session_.Post();
        if (cpr_resp.status_code == 200) {
            if (!resp.ParseFromString(cpr_resp.text)) {
                FLOG_ERROR("[Master] post {} parse response failed.", url);
                ret = Status(Status::kCorruption, "parse response", EncodeToHex(cpr_resp.text));
            } else {
                FLOG_DEBUG("[Master] post {} response: {}", url, resp.ShortDebugString());
                return Status::OK();
            }
        } else {
            ret = Status(Status::kUnknown, "post " + url, std::to_string(cpr_resp.status_code));
            FLOG_ERROR("[Master] post {} status code: {}", url, cpr_resp.status_code);
        }
    }
    return ret;
}

template <class ResponseT>
static Status checkHeader(const ResponseT& resp) {
    if (resp.has_header() && resp.header().has_error()) {
        return Status(Status::kUnknown, "header error", resp.header().error().ShortDebugString());
    } else {
        return Status::OK();
    }
}

Status MasterClientImpl::NodeRegister(mspb::RegisterNodeRequest &req, mspb::RegisterNodeResponse &resp) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    auto s = post(kRegisterURI, req, resp);
    if (!s.ok()) {
        return s;
    }
    return checkHeader(resp);
}

Status MasterClientImpl::nodeGet(mspb::GetNodeRequest &req, mspb::GetNodeResponse &resp) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    auto s = post(kNodeGetURI, req, resp);
    if (!s.ok()) {
        return s;
    }
    return checkHeader(resp);
}

Status MasterClientImpl::GetRaftAddress(uint64_t node_id, std::string *addr) {
    mspb::GetNodeRequest req;
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_id(node_id);

    mspb::GetNodeResponse resp;
    auto s = nodeGet(req, resp);
    if (!s.ok()) {
        return s;
    }
    if (addr != nullptr) {
        *addr = resp.node().ip() + ":" + std::to_string(resp.node().raft_port());
    }
    return Status::OK();
}

Status MasterClientImpl::NodeHeartbeat(mspb::NodeHeartbeatRequest &req, mspb::NodeHeartbeatResponse &resp) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    auto s = post(kNodeHeartbeatURI, req, resp);
    if (!s.ok()) {
        return s;
    }
    return checkHeader(resp);
}

Status MasterClientImpl::RangeHeartbeat(mspb::RangeHeartbeatRequest &req, mspb::RangeHeartbeatResponse &resp) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    auto s = post(kRangeHeartbeatURI, req, resp);
    if (!s.ok()) {
        return s;
    }
    return checkHeader(resp);
}

Status MasterClientImpl::AskSplit(mspb::AskSplitRequest &req, mspb::AskSplitResponse &resp) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    auto s = post(kAskSplitURI, req, resp);
    if (!s.ok()) {
        return s;
    }
    return checkHeader(resp);
}

} // namespace master
} // namespace ds
} // namespace jimkv
