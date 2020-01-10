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

_Pragma("once");

#include "raft/include/raft/options.h"

namespace jimkv {
namespace raft {
namespace impl {

class SnapWorkerPool;
class SendSnapTask;
class ApplySnapTask;

class SnapshotManager final {
public:
    explicit SnapshotManager(const SnapshotOptions& opt);
    ~SnapshotManager();

    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;

    Status Dispatch(const std::shared_ptr<SendSnapTask>& send_task);
    Status Dispatch(const std::shared_ptr<ApplySnapTask>& apply_task);

    uint64_t SendingCount() const;
    uint64_t ApplyingCount() const;

    std::string GetSendingDesc() const;
    std::string GetApplyingDesc() const;

private:
    const SnapshotOptions opt_;

    std::unique_ptr<SnapWorkerPool> send_work_pool_;
    std::unique_ptr<SnapWorkerPool> apply_work_pool_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
