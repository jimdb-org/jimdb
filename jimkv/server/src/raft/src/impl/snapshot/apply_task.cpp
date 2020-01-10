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

#include "apply_task.h"

#include <sstream>

namespace jimkv {
namespace raft {
namespace impl {

ApplySnapTask::ApplySnapTask(const SnapContext& context,
                             const std::shared_ptr<StateMachine>& sm)
    : SnapTask(context), sm_(sm) {}

ApplySnapTask::~ApplySnapTask() = default;

Status ApplySnapTask::RecvData(const MessagePtr& msg) {
    assert(msg->id() == GetContext().id);
    assert(msg->type() == pb::SNAPSHOT_REQUEST);

    if (msg->from() != GetContext().from) {
        return Status(Status::kInvalidArgument, "from",
                      std::to_string(GetContext().from));
    } else if (msg->term() != GetContext().term) {
        return Status(Status::kInvalidArgument, "term",
                      std::to_string(GetContext().term));
    } else if (msg->snapshot().uuid() != GetContext().uuid) {
        return Status(Status::kInvalidArgument, "uuid",
                      std::to_string(GetContext().uuid));
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (next_data_ != nullptr) {
            return Status(Status::kExisted, "prev block has not yet applied",
                          std::to_string(next_data_->snapshot().seq()));
        } else {
            next_data_ = msg;
        }
    }
    cv_.notify_one();

    return Status::OK();
}

void ApplySnapTask::run(SnapResult* result) {
    assert(sm_ != nullptr);
    assert(transport_ != nullptr);
    assert(opt_.wait_data_timeout_secs != 0);

    bool over = false;
    while (!over) {
        if (IsCanceled()) {
            result->status = Status(Status::kAborted, "canceled", "");
            return;
        }

        MessagePtr data;
        result->status = waitNextData(&data);
        if (!result->status.ok()) {
            return;
        }

        size_t bytes = data->ByteSizeLong();
        result->status = applyData(data, over);
        if (!result->status.ok()) {
            return;
        }
        result->bytes_count += bytes;
        result->blocks_count++;

        sendAck(data->snapshot().seq(), !result->status.ok());
    }
}

Status ApplySnapTask::waitNextData(MessagePtr* data) {
    std::unique_lock<std::mutex> lock(mu_);
    if (cv_.wait_for(lock, std::chrono::seconds(opt_.wait_data_timeout_secs),
                     [this] { return next_data_ != nullptr || canceled_; })) {
        if (canceled_) {
            return Status(Status::kAborted, "canceled", "");
        } else {
            *data = std::move(next_data_);
            next_data_.reset();
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait next data block",
                      std::to_string(prev_seq_ + 1));
    }
}

Status ApplySnapTask::applyData(MessagePtr data, bool& over) {
    auto snapshot = data->snapshot();

    auto seq = snapshot.seq();
    if (seq != prev_seq_ + 1) {
        return Status(Status::kInvalidArgument, "discontinuous seq",
                      std::string("prev:") + std::to_string(prev_seq_.load()) +
                      ", msg:" + std::to_string(seq));
    }

    if (seq == 1) {
        snap_index_ = snapshot.meta().index();
        auto s = sm_->ApplySnapshotStart(snapshot.meta().context(), snap_index_);
        if (!s.ok()) return s;
    }

    if (snapshot.datas_size() > 0) {
        std::vector<std::string> datas;
        datas.resize(static_cast<size_t>(snapshot.datas_size()));
        for (int i = 0; i < snapshot.datas_size(); ++i) {
            data->mutable_snapshot()->mutable_datas(i)->swap(datas[i]);
        }
        auto s = sm_->ApplySnapshotData(datas);
        if (!s.ok()) return s;
    }

    over = snapshot.final();
    if (over) {
        auto s = sm_->ApplySnapshotFinish(snap_index_);
        if (!s.ok()) return s;
    }

    ++prev_seq_;

    return Status::OK();
}

void ApplySnapTask::sendAck(int64_t seq, bool reject) {
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::SNAPSHOT_ACK);
    msg->set_id(GetContext().id);
    msg->set_from(GetContext().to);
    msg->set_to(GetContext().from);
    msg->set_term(GetContext().term);
    msg->set_reject(reject);
    msg->mutable_snapshot()->set_uuid(GetContext().uuid);
    msg->mutable_snapshot()->set_seq(seq);

    transport_->SendMessage(msg);
}

void ApplySnapTask::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        canceled_ = true;
    }

    cv_.notify_one();
}

std::string ApplySnapTask::Description() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"id\": " << GetContext().id << ", ";
    ss << "\"from\": " << GetContext().from << ", ";
    ss << "\"term\": " << GetContext().term << ", ";
    ss << "\"uuid\": " << GetContext().uuid << ",";
    ss << "\"seq\": " << prev_seq_;
    ss << "}";
    return ss.str();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
