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

#include <atomic>
#include <memory>

#include "types.h"

namespace jim {
namespace raft {
namespace impl {

class SnapTask {
public:
    explicit SnapTask(const SnapContext& ctx) :
            context_(ctx),
            id_(std::to_string(ctx.id) + "/" + std::to_string(ctx.term) + "/" + std::to_string(ctx.uuid))
    {
    }

    virtual ~SnapTask() = default;

    SnapTask(const SnapTask&) = delete;
    SnapTask& operator=(const SnapTask&) = delete;

    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

    const std::string& ID() const { return id_; }

    const SnapContext& GetContext() const { return context_; }

    void SetReporter(const SnapReporter& reporter) { reporter_ = reporter; }

    void Run() {
        assert(reporter_);

        SnapResult result;
        this->run(&result);
        reporter_(context_, result);
    }

    virtual void Cancel() = 0;

    virtual std::string Description() const = 0;

protected:
    virtual void run(SnapResult *result) = 0;

private:
    const SnapContext context_;
    const std::string id_; // unique task id

    std::atomic<bool> dispatched_ = {false};
    SnapReporter reporter_;
};

using SnapTaskPtr = std::shared_ptr<SnapTask>;

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
