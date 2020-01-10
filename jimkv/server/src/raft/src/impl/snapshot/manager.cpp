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

#include "manager.h"

#include <sstream>
#include "send_task.h"
#include "apply_task.h"
#include "worker_pool.h"

namespace jimkv {
namespace raft {
namespace impl {

SnapshotManager::SnapshotManager(const SnapshotOptions& opt)
    : opt_(opt),
      send_work_pool_(new SnapWorkerPool("snap_send", opt_.max_send_concurrency)),
      apply_work_pool_(new SnapWorkerPool("snap_apply", opt_.max_apply_concurrency)) {}

SnapshotManager::~SnapshotManager() = default;

Status SnapshotManager::Dispatch(const std::shared_ptr<SendSnapTask>& send_task) {
    if(send_work_pool_->Post(send_task)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy, "max send snapshot concurrency limit reached",
                      std::to_string(send_work_pool_->RunningsCount()));
    }
}

Status SnapshotManager::Dispatch(const std::shared_ptr<ApplySnapTask>& apply_task) {
    if(apply_work_pool_->Post(apply_task)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy, "max apply snapshot concurrency limit reached",
                      std::to_string(apply_work_pool_->RunningsCount()));
    }
}

uint64_t SnapshotManager::SendingCount() const {
    return send_work_pool_->RunningsCount();

}

uint64_t SnapshotManager::ApplyingCount() const {
    return apply_work_pool_->RunningsCount();
}

static std::string getTasksDesc(const std::string& type,
                                const std::vector<SnapTaskPtr>& tasks) {
    std::ostringstream ss;
    ss << "{";
    ss << "\" << type << \" : [";
    for (size_t i = 0; i < tasks.size(); ++i) {
        ss << tasks[i]->Description();
        if (i != tasks.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]}";
    return ss.str();
};

std::string SnapshotManager::GetSendingDesc() const {
    std::vector<SnapTaskPtr> tasks;
    send_work_pool_->GetRunningTasks(&tasks);
    return getTasksDesc("send", tasks);
}

std::string SnapshotManager::GetApplyingDesc() const {
    std::vector<SnapTaskPtr> tasks;
    apply_work_pool_->GetRunningTasks(&tasks);
    return getTasksDesc("apply", tasks);
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
