// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JIMDB Authors.
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

#include "raft_impl.h"

#include <future>

#include "common/logger.h"
#include "raft_exception.h"
#include "raft_fsm.h"
#include "raft_types.h"

#include "snapshot/apply_task.h"
#include "snapshot/send_task.h"
#include "storage/storage.h"

namespace jim {
namespace raft {
namespace impl {

RaftImpl::RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
        const RaftContext& ctx) :
    sops_(sops),
    ops_(ops),
    ctx_(ctx),
    fsm_(new RaftFsm(sops, ctx.mutable_options, ops)) {
    initPublish();
}

RaftImpl::~RaftImpl() { Stop(); }

void RaftImpl::initPublish() {
    uint64_t leader = 0, term = 0;
    std::tie(leader, term) = fsm_->GetLeaderTerm();
    bulletin_board_.PublishLeaderTerm(leader, term);
    bulletin_board_.PublishPeers(fsm_->GetPeers());
    bulletin_board_.PublishStatus(fsm_->GetStatus());
}

Status RaftImpl::TryToLeader() {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                std::to_string(ops_.id));
    }
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_HUP);
    msg->set_from(sops_.node_id);
    RecvMsg(msg);
    return Status::OK();
}

Work RaftImpl::wrapWork(const std::function<void()>& f) {
    auto self = shared_from_this();
    return [self, f]() {
        if (self->IsStopped()) {
            return;
        }
        try {
            f();
        } catch (RaftException& e) {
            FLOG_ERROR("raft[{}] throw an exception: {}. removed.", self->ops_.id, e.what());
            self->ctx_.server_->RemoveRaft(self->ops_.id);
        }
    };
}

void RaftImpl::post(const std::function<void()>& f) {
    ctx_.consensus_thread->post(wrapWork(f));
}

bool RaftImpl::tryPost(const std::function<void()>& f) {
    return ctx_.consensus_thread->tryPost(wrapWork(f));
}

Status RaftImpl::Propose(std::string& entry_data, uint32_t entry_flags) {
    // check service availability
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }
    if (ctx_.consensus_thread->isFull()) {
        return Status(Status::kBusy);
    }

    // create entry
    pb::Entry entry;
    entry.mutable_data()->swap(entry_data);
    entry.set_type(pb::ENTRY_NORMAL);
    if (entry_flags != 0) {
        entry.set_flags(entry_flags);
    }

    // push to queue, merge entry for the purpose of batch if possible
    bool can_merge = false;
    {
        std::lock_guard<std::mutex> lock(propose_lock_);
        can_merge = !propose_que_.empty() && propose_que_.back()->entries_size() < sops_.entry_batch_size;
        if (!can_merge) {
            MessagePtr msg(new pb::Message);
            msg->set_type(pb::LOCAL_MSG_PROP);
            propose_que_.push_back(std::move(msg));
        }
        propose_que_.back()->add_entries()->Swap(&entry);
    }

    // if can not merge to a exist work, schedule to run a new work to handle the newcome entry
    if (!can_merge) {
        post([this] {
            MessagePtr msg;
            {
                std::lock_guard<std::mutex> lock(propose_lock_);
                if (!propose_que_.empty()) {
                    msg = propose_que_.front();
                    propose_que_.pop_front();
                }
            }
            if (msg) {
                Step(msg);
            }
        });
    }
    return Status::OK();
}

Status RaftImpl::ReadIndex(std::string& ctx) {
    // check service availability
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }
    if (ctx_.consensus_thread->isFull()) {
        return Status(Status::kBusy);
    }

    // create entry
    pb::Entry entry;
    entry.mutable_data()->swap(ctx);

    // push to queue, merge entry for the purpose of batch if possible
    bool can_merge = false;
    {
        std::lock_guard<std::mutex> lock(read_index_lock_);
        can_merge = !read_index_que_.empty() && read_index_que_.back()->entries_size() < sops_.entry_batch_size;
        if (!can_merge) {
            MessagePtr msg(new pb::Message);
            msg->set_type(pb::LOCAL_MSG_READ_INDEX);
            read_index_que_.push_back(std::move(msg));
        }
        read_index_que_.back()->add_entries()->Swap(&entry);
    }

    // if can not merge to a exist work, schedule to run a new work to handle the newcome requests
    if (!can_merge) {
        post([this] {
            MessagePtr msg;
            {
                std::lock_guard<std::mutex> lock(read_index_lock_);
                if (!read_index_que_.empty()) {
                    msg = read_index_que_.front();
                    read_index_que_.pop_front();
                }
            }
            if (msg) {
                Step(msg);
            }
        });
    }
    return Status::OK();
}

Status RaftImpl::ChangeMemeber(const ConfChange& conf) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                std::to_string(ops_.id));
    }

    std::string str;
    auto s = EncodeConfChange(conf, &str);
    if (!s.ok()) return s;

    auto msg = std::make_shared<pb::Message>();
    msg->set_type(pb::LOCAL_MSG_PROP);
    auto entry = msg->add_entries();
    entry->set_type(pb::ENTRY_CONF_CHANGE);
    entry->set_data(std::move(str));

    if (tryPost(std::bind(&RaftImpl::Step, this, msg))) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }
}

void RaftImpl::Truncate(uint64_t index) {
    FLOG_DEBUG("node_id: {} raft[{}] truncate {}", sops_.node_id, ops_.id, index);
    post([=] { fsm_->TruncateLog(index); });
}

void RaftImpl::RecvMsg(MessagePtr msg) {
#ifdef JIMKV_RAFT_TRACE_MSG
    if (msg->type() != pb::LOCAL_MSG_TICK) {
        FLOG_DEBUG("node_id: {} raft[{}] recv msg type: {} from {}, term: {} at local term: {}",
                   fsm_->node_id_, fsm_->id_, pb::MessageType_Name(msg->type()), msg->from(),
                   msg->term(), fsm_->term_);
    } else {
        FLOG_DEBUG("node_id: {}, raft[{}] recv msg type: LOCAL_MSG_TICK messages term: {} at local term: {}",
                   fsm_->node_id_, fsm_->id_, msg->term(), fsm_->term_);
    }
#endif
    if (stopped_) return;

    if (!tryPost(std::bind(&RaftImpl::Step, this, msg))) {
        FLOG_WARN("node_id: {} raft[{}] discard a msg. type: {} from {}, term: {}",
                  sops_.node_id, ops_.id, pb::MessageType_Name(msg->type()), msg->from(), msg->term());
    }
}

void RaftImpl::Tick(MessagePtr msg) {
    ++tick_count_;
    RecvMsg(msg);
}

void RaftImpl::Step(MessagePtr msg) {
    if (!fsm_->Validate(msg)) {
        FLOG_WARN("node_id: {} raft[{}] ignore invalidate msg type: {} from {}, term: {}",
                  sops_.node_id, ops_.id, pb::MessageType_Name(msg->type()), msg->from(), msg->term());
        return;
    }

    fsm_->Step(msg);
    fsm_->GetReady(&ready_);

    if (!ready_.msgs.empty()) sendMessages();

    if (ready_.send_snap) sendSnapshot();

    if (ready_.apply_snap) applySnapshot();

    persist();

    apply();

    publish();
}

void RaftImpl::sendMessages() {
    for (auto m : ready_.msgs) {
        ctx_.msg_sender->SendMessage(m);
    }
}

void RaftImpl::sendSnapshot() {
    auto task = ready_.send_snap;
    assert(task != nullptr);

    task->SetReporter(std::bind(&RaftImpl::ReportSnapSendResult, shared_from_this(),
                std::placeholders::_1, std::placeholders::_2));
    task->SetTransport(ctx_.msg_sender);

    SendSnapTask::Options send_opt;
    send_opt.max_size_per_msg = sops_.snapshot_options.max_size_per_msg;
    send_opt.wait_ack_timeout_secs = sops_.snapshot_options.ack_timeout_seconds;
    task->SetOptions(send_opt);

    auto s = ctx_.snapshot_manager->Dispatch(task);
    if (!s.ok()) {
        SnapResult result;
        result.status = s;
        ReportSnapSendResult(task->GetContext(), result);
    }
}

void RaftImpl::applySnapshot() {
    auto task = ready_.apply_snap;
    assert(task != nullptr);

    task->SetReporter(std::bind(&RaftImpl::ReportSnapApplyResult, shared_from_this(),
                std::placeholders::_1, std::placeholders::_2));
    task->SetTransport(ctx_.msg_sender);

    ApplySnapTask::Options apply_opt;
    // TODO: use a config
    apply_opt.wait_data_timeout_secs = 10;
    task->SetOptions(apply_opt);

    auto s = ctx_.snapshot_manager->Dispatch(task);
    if (!s.ok()) {
        SnapResult result;
        result.status = s;
        ReportSnapApplyResult(task->GetContext(), result);
    }
}

void RaftImpl::apply() {
    auto failed_ents = ready_.failed_entries;
    for (const auto& e : failed_ents) {
        fsm_->sm_->OnReplicateError(e->data(), Status(Status::kNotLeader));
    }

    const auto& ents = ready_.committed_entries;
    for (const auto& e : ents) {
        if (e->type() == pb::ENTRY_CONF_CHANGE) {
            auto s = fsm_->applyConfChange(e);
            if (!s.ok()) {
                throw RaftException(std::string("apply confchange[") +
                        std::to_string(e->index()) + "] error: " +
                        s.ToString());
            }
            conf_changed_ = true;
        }
        smApply(e);
    }
    if (!ents.empty()) {
        fsm_->appliedTo(ents[ents.size()-1]->index());
    }
}

void RaftImpl::persist() {
    auto hs = fsm_->GetHardState();
    bool hs_changed = prev_hard_state_.term() != hs.term() ||
        prev_hard_state_.vote() != hs.vote() ||
        prev_hard_state_.commit() != hs.commit();
    if (hs_changed) {
        prev_hard_state_ = hs;
    }
    auto s = fsm_->Persist(hs_changed);
    if (!s.ok()) throw RaftException(s);
}

void RaftImpl::publish() {
    bool leader_changed = false;
    uint64_t leader = 0, term = 0;
    std::tie(leader, term) = fsm_->GetLeaderTerm();
    if (leader != bulletin_board_.Leader() || term != bulletin_board_.Term()) {
        bulletin_board_.PublishLeaderTerm(leader, term);
        leader_changed = true;
    }

    if (conf_changed_) {
        bulletin_board_.PublishPeers(fsm_->GetPeers());
    }

    if (conf_changed_ || leader_changed || tick_count_ % sops_.status_tick == 0) {
        bulletin_board_.PublishStatus(fsm_->GetStatus());
    }
    conf_changed_ = false;

    if (leader_changed) {
        ops_.statemachine->OnLeaderChange(leader, term);
    }
}

void RaftImpl::ReportSnapSendResult(const SnapContext& ctx, const SnapResult& result) {
    if (result.status.ok()) {
        FLOG_INFO("node_id: {} raft[{}] send snapshot[uuid: {}] to {} finished. total blocks: {}, bytes: {}",
                  sops_.node_id, ops_.id, ctx.uuid, ctx.to, result.blocks_count, result.bytes_count);
    } else {
        FLOG_ERROR("node_id: {} raft[{}] send snapshot[uuid: {}] to {} failed({}). sent blocks: {}, bytes: {}",
                   sops_.node_id, ops_.id, ctx.uuid, ctx.to, result.status.ToString(),
                   result.blocks_count, result.bytes_count);
    }

    if (IsStopped()) return;

    MessagePtr resp(new pb::Message);
    resp->set_type(pb::LOCAL_SNAPSHOT_STATUS);
    resp->set_to(ctx.from);
    resp->set_from(ctx.to);
    resp->set_term(ctx.term);
    resp->set_reject(!result.status.ok());
    resp->mutable_snapshot()->set_uuid(ctx.uuid);

    post(std::bind(&RaftImpl::Step, this, resp));
}

void RaftImpl::ReportSnapApplyResult(const SnapContext& ctx, const SnapResult& result) {
    if (result.status.ok()) {
        FLOG_INFO("node_id: {} raft[{}] apply snapshot[uuid: {}] from {} finished. total blocks: {}, bytes: {}",
                  sops_.node_id, ops_.id, ctx.uuid, ctx.from, result.blocks_count, result.bytes_count);
    } else {
        FLOG_ERROR("node_id: {} raft[{}] apply snapshot[uuid: {}] from {} failed({}). sent blocks: {}, bytes: {}",
                   sops_.node_id, ops_.id, ctx.uuid, ctx.from, result.status.ToString(),
                   result.blocks_count, result.bytes_count);
    }

    if (IsStopped()) return;

    MessagePtr resp(new pb::Message);
    resp->set_type(pb::LOCAL_SNAPSHOT_STATUS);
    resp->set_to(sops_.node_id);
    resp->set_from(ctx.from);
    resp->set_term(ctx.term);
    resp->set_reject(!result.status.ok());
    resp->mutable_snapshot()->set_uuid(ctx.uuid);

    post(std::bind(&RaftImpl::Step, this, resp));
}

void RaftImpl::smApply(const EntryPtr& e) {
    auto s = fsm_->smApply(e);
    if (!s.ok()) {
        throw RaftException(std::string("statemachine apply entry[") +
                std::to_string(e->index()) + "] error: " + s.ToString());
    }
}

void RaftImpl::Stop() {
    stopped_ = true;
}

Status RaftImpl::Destroy(bool backup) {
    FLOG_WARN("node_id: {} raft[{}] destroy log storage", sops_.node_id, ops_.id);

    return fsm_->DestroyLog(backup);
}

std::unique_ptr<LogReader> RaftImpl::ReadLog(uint64_t start_index) {
    if (IsStopped()) {
        return nullptr;
    }

    if (ctx_.consensus_thread->inCurrentThread()) {
        return fsm_->storage_->NewReader(start_index);
    }

    auto p = new std::promise<std::unique_ptr<LogReader>>();
    auto f = p->get_future();
    auto self = shared_from_this();
    ctx_.consensus_thread->post([=]() {
        p->set_value(self->IsStopped() ? nullptr : self->fsm_->storage_->NewReader(start_index));
        delete p;
    });
    auto ws = f.wait_for(std::chrono::seconds(5));
    if (ws == std::future_status::ready) {
        return f.get();
    } else {
        return nullptr;
    }
}

Status RaftImpl::InheritLog(const std::string& dir, uint64_t last_index, bool only_index) {
    if (IsStopped()) {
        return Status(Status::kShutdownInProgress);
    }

    if (ctx_.consensus_thread->inCurrentThread()) {
        return fsm_->storage_->InheritLog(dir, last_index, only_index);
    }

    auto p = new std::promise<Status>();
    auto f = p->get_future();
    auto self = shared_from_this();
    ctx_.consensus_thread->post([=]() {
        p->set_value(self->IsStopped() ? Status(Status::kShutdownInProgress) :
                     self->fsm_->storage_->InheritLog(dir, last_index, only_index));
        delete p;
    });
    auto ws = f.wait_for(std::chrono::seconds(5));
    if (ws == std::future_status::ready) {
        return f.get();
    } else {
        return Status(Status::kTimedOut);
    }
}

}  // namespace impl
}  // namespace raft
} /* namespace jim */
