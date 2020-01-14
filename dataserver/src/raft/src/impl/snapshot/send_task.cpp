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

#include "send_task.h"

#include <sstream>

namespace jim {
namespace raft {
namespace impl {

SendSnapTask::SendSnapTask(const SnapContext& context, pb::SnapshotMeta&& meta,
                           const std::shared_ptr<Snapshot>& data)
    : SnapTask(context),
      meta_(std::move(meta)),
      data_(data) {
}

SendSnapTask::~SendSnapTask() {}

Status SendSnapTask::RecvAck(MessagePtr& msg) {
    assert(msg->type() == pb::SNAPSHOT_ACK);
    assert(msg->id() == GetContext().id);

    if (msg->snapshot().uuid() != GetContext().uuid) {
        return Status(Status::kInvalidArgument, "uuid", std::to_string(GetContext().uuid));
    } else if (msg->term() != GetContext().term) {
        return Status(Status::kInvalidArgument, "term", std::to_string(GetContext().term));
    } else if (msg->from() != GetContext().to) {
        return Status(Status::kInvalidArgument, "from", std::to_string(GetContext().to));
    }

    auto seq = msg->snapshot().seq();
    auto reject = msg->reject();
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (seq <= ack_seq_) {
            return Status(Status::kInvalidArgument, "stale ack seq", std::to_string(ack_seq_));
        }
        ack_seq_ = seq;
        rejected_ = reject;
    }
    cv_.notify_one();

    return Status::OK();
}

void SendSnapTask::run(SnapResult* result) {
    assert(transport_ != nullptr);
    assert(opt_.max_size_per_msg != 0);
    assert(opt_.wait_ack_timeout_secs != 0);

    if (IsCanceled()) {
        result->status = Status(Status::kAborted);
        return;
    }

    // create connection
    std::shared_ptr<transport::Connection> conn;
    result->status = transport_->GetConnection(GetContext().to, &conn);
    if (!result->status.ok()) {
        return;
    }

    bool over = false;
    int64_t seq = 0;
    while (!over) {
        if (IsCanceled()) {
            result->status = Status(Status::kAborted, "canceled", "");
            return;
        }

        ++seq;

        MessagePtr msg(new pb::Message);
        result->status = nextMsg(seq, msg, over);
        if (!result->status.ok()) {
            return;
        }

        size_t size = msg->ByteSizeLong();
        result->status = conn->Send(msg);
        if (!result->status.ok()) {
            return;
        }
        result->blocks_count += 1;
        result->bytes_count += size;

        result->status = waitAck(seq, opt_.wait_ack_timeout_secs);
        if (!result->status.ok()) {
            return;
        }
    }
}

Status SendSnapTask::waitAck(int64_t seq, size_t timeout_secs) {
    std::unique_lock<std::mutex> lock(mu_);
    if (cv_.wait_for(lock, std::chrono::seconds(timeout_secs),
                     [seq, this] { return ack_seq_ >= seq || canceled_; })) {
        if (canceled_) {
            return Status(Status::kAborted, "canceled", "");
        } else if (rejected_) {
            return Status(Status::kAborted, "reject", std::to_string(seq));
        } else {
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait ack", std::to_string(seq));
    }
}

Status SendSnapTask::nextMsg(int64_t seq, MessagePtr& msg, bool& over) {
    msg->set_type(pb::SNAPSHOT_REQUEST);
    msg->set_id(GetContext().id);
    msg->set_to(GetContext().to);
    msg->set_from(GetContext().from);
    msg->set_term(GetContext().term);

    auto snapshot = msg->mutable_snapshot();
    snapshot->set_uuid(GetContext().uuid);
    snapshot->set_seq(seq);

    if (seq == 1) {
        snapshot->mutable_meta()->Swap(&meta_);
        over = false;
        return Status::OK();
    }

    uint64_t size = 0;
    while (!over && size < opt_.max_size_per_msg) {
        if (IsCanceled()) {
            return Status(Status::kAborted, "canceled", "");
        }

        std::string data;
        auto s = data_->Next(&data, &over);
        if (!s.ok()) return s;
        if (data.empty()) continue;

        size += data.size();
        snapshot->add_datas()->swap(data);
    }

    snapshot->set_final(over);

    return Status::OK();
}

void SendSnapTask::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        canceled_ = true;
    }
    cv_.notify_one();
}

std::string SendSnapTask::Description() const {
    std::ostringstream ss;
    ss << "{";
    ss << "\"id\": " << GetContext().id << ", ";
    ss << "\"to\": " << GetContext().to<< ", ";
    ss << "\"term\": " << GetContext().term << ", ";
    ss << "\"uuid\": " << GetContext().uuid << ",";
    ss << "\"ack\": " << ack_seq_;
    ss << "}";
    return ss.str();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
