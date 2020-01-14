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

#include "jemalloc/jemalloc.h"
#include "db/rocksdb_impl/manager_impl.h"

#include "base/util.h"

namespace jim {
namespace ds {
namespace admin {

using namespace dspb;

Status profileHeap(const ProfileRequest& req) {
    auto path = req.output_path();
    if (path.empty()) {
        char tmp[PATH_MAX] = {'\0'};
        unsigned pid = getpid();
        unsigned timestamp = ::time(NULL);
        snprintf(tmp, PATH_MAX, "/tmp/ds.%u.%u.pprof", pid, timestamp);
        path = tmp;
    }
    const char *file_name = path.c_str();
    auto ret = mallctl("prof.dump", NULL, NULL, &file_name, sizeof(const char *));
    if (ret != 0) {
        return Status(Status::kUnknown, "mallctl", strErrno(ret));
    }
    return Status::OK();
}

Status AdminServer::profile(const ProfileRequest& req, ProfileResponse *resp) {
    switch (req.ptype()) {
    case ProfileRequest_ProfileType_CPU:
        return Status(Status::kNotSupported, "profile", "cpu");
    case ProfileRequest_ProfileType_HEAP:
        return profileHeap(req);
    case ProfileRequest_ProfileType_ROCKSDB: {
        auto ret = GetRocksdbMgr(context_);
        if (!ret.second.ok()) {
            return ret.second;
        }
        switch (req.op()) {
            case dspb::ProfileRequest_ProfileOp_PROFILE_START:
                return ret.first->SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
                break;
            case dspb::ProfileRequest_ProfileOp_PROFILE_STOP:
                return ret.first->SetPerfLevel(rocksdb::PerfLevel::kDisable);
                break;
            default:
                return Status(Status::kInvalidArgument, "profile op type", std::to_string(req.op()));
        }
        break;
    }
    default:
        return Status(Status::kNotSupported, "profile",
                ProfileRequest_ProfileType_Name(req.ptype()));
    }
}

} // namespace admin
} // namespace ds
} // namespace jim
