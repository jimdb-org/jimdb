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

#include "jimkv_master.h"

#include <curl/curl.h>
#include "cpr/cpr.h"

#include "mspb/mspb.pb.h"

#include "base/util.h"
#include "jimkv_log.h"

namespace jimkv {
namespace sdk {

static const std::string kProtocalSchema = "http://";
static const cpr::Header kHTTPHeader {{"Content-Type", "application/proto"}};
static const std::string kNodeGetURI = "/node/get";
static const std::string kGetRouteURI = "/range/routers";

JimMaster::JimMaster(uint64_t cluster_id, const std::string &host) :
    cluster_id_(cluster_id),
    host_(host)
{
}

bool JimMaster::GetRoute(mspb::GetRouteRequest& req, RoutePred pred) {
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_rangetype(basepb::RangeType::RNG_Data);

    CBLOG_INFO("[Master] GetRoute %" PRIu64, cluster_id_);
    mspb::GetRouteResponse resp;
    if (Post(kGetRouteURI, req, &resp) && CheckHeader(resp.header())) {
        return pred(resp);
    }

    return false;
}

bool JimMaster::GetNode(uint64_t node_id, NodePred pred) {
    mspb::GetNodeRequest req;
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_id(node_id);

    CBLOG_INFO("[Master] GetNode %" PRIu64, node_id);
    mspb::GetNodeResponse resp;
    if (Post(kNodeGetURI, req, &resp) && CheckHeader(resp.header())) {
        return pred(resp);
    }

    return false;
}

bool JimMaster::CheckHeader(const mspb::ResponseHeader& header) {
    if (header.has_error()) {
        CBLOG_ERROR("[Master] post error: %s", header.error().message().c_str());
        return false;
    }
    return true;
}

bool JimMaster::Post(const std::string &uri, const Req& req, Resp* resp) {
    cpr::Body req_body{req.SerializeAsString()};

    std::string url = kProtocalSchema + host_ + uri;
    cpr::Url cpr_url{url};

    cpr::Response cpr_resp = cpr::Post(cpr_url, kHTTPHeader, req_body);
    if (cpr_resp.status_code == 200) {
        if (!resp->ParseFromString(cpr_resp.text)) {
            CBLOG_ERROR("[Master] post %s parse response failed.", url.c_str());
            return false;
        }
        return true;
    }

    CBLOG_ERROR("[Master] post %s status code: %d", url.c_str(), cpr_resp.status_code);
    return false;
}


} // namespace sdk
} // namespace jimkv

