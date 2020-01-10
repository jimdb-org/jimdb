// Copyright 2015 The etcd Authors
// Portions Copyright 2019 The JimDB Authors.
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

#include "server_impl.h"

#include <thread>

#include "common/logger.h"
#include "raft_exception.h"
#include "raft_impl.h"
#include "snapshot/manager.h"
#include "transport/inprocess_transport.h"
#include "transport/tcp_transport.h"

namespace jimkv {
namespace raft {
namespace impl {

RaftServerImpl::RaftServerImpl(const RaftServerOptions& ops) : ops_(ops) {
    tick_msg_.reset(new pb::Message);
    tick_msg_->set_type(pb::LOCAL_MSG_TICK);
}

RaftServerImpl::~RaftServerImpl() {
    Stop();

    for (auto t: consensus_threads_) {
        delete t;
    }
}

Status RaftServerImpl::Start() {
    auto status = ops_.Validate();
    if (!status.ok()) {
        return status;
    }

    for (int i = 0; i < ops_.consensus_threads_num; ++i) {
        auto t = new WorkThread(ops_.consensus_queue_capacity,
                                std::string("raft-worker:") + std::to_string(i));
        consensus_threads_.push_back(t);
    }
    FLOG_INFO("raft[server] {} consensus threads start. queue capacity={}",
             ops_.consensus_threads_num, ops_.consensus_queue_capacity);

    // start transport
    if (ops_.transport_options.use_inprocess_transport) {
        transport_.reset(new transport::InProcessTransport(ops_.node_id));
    } else {
        transport_.reset(new transport::TcpTransport(ops_.transport_options));
    }
    status = transport_->Start(
        ops_.transport_options.listen_ip, ops_.transport_options.listen_port,
        std::bind(&RaftServerImpl::onMessage, this, std::placeholders::_1));
    if (!status.ok()) {
        return status;
    }

    // start snapshot sender
    assert(snapshot_manager_ == nullptr);
    snapshot_manager_.reset(new SnapshotManager(ops_.snapshot_options));

    running_ = true;
    tick_thr_.reset(new std::thread([this]() {
        tickRoutine(); }));

    return Status::OK();
}

Status RaftServerImpl::Stop() {
    if (!running_) return Status::OK();

    running_ = false;

    if (tick_thr_ && tick_thr_->joinable()) {
        tick_thr_->join();
    }

    for (auto& t : consensus_threads_) {
        t->shutdown();
    }

    if (snapshot_manager_ != nullptr) {
        snapshot_manager_.reset(nullptr);
    }

    if (transport_ != nullptr) {
        transport_->Shutdown();
    }

    return Status::OK();
}

Status RaftServerImpl::CreateRaft(const RaftOptions& ops, std::shared_ptr<Raft>* raft) {
    auto status = ops.Validate();
    if (!status.ok()) {
        return status;
    }

    uint64_t counter = 0;
    {
        std::unique_lock<jimkv::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.find(ops.id);
        if (it != all_rafts_.end()) {
            return Status(Status::kDuplicate, "create raft", std::to_string(ops.id));
        }
        auto ret = creating_rafts_.insert(ops.id);
        if (!ret.second) {
            return Status(Status::kDuplicate, "raft is creating", std::to_string(ops.id));
        }
        counter = create_count_++;
    }

    RaftContext ctx;
    ctx.server_ = this;
    ctx.msg_sender = transport_.get();
    ctx.snapshot_manager = snapshot_manager_.get();
    ctx.consensus_thread = consensus_threads_[counter % consensus_threads_.size()];
    ctx.mutable_options = &mutable_ops_;

    std::shared_ptr<RaftImpl> r;
    try {
        r = std::make_shared<RaftImpl>(ops_, ops, ctx);
    } catch (RaftException& e) {
        {
            std::unique_lock<jimkv::shared_mutex> lock(rafts_mu_);
            creating_rafts_.erase(ops.id);
        }
        return Status(Status::kUnknown, "create raft", e.what());
    }

    assert(r != nullptr);
    {
        std::unique_lock<jimkv::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.emplace(ops.id, r);
        assert(it.second);
        (void)it;
        creating_rafts_.erase(ops.id);
    }
    *raft = std::static_pointer_cast<Raft>(r);

    return Status::OK();
}

Status RaftServerImpl::RemoveRaft(uint64_t id) {
    FLOG_WARN("remove raft[{}]", id);

    std::shared_ptr<RaftImpl> r;
    {
        std::unique_lock<jimkv::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.find(id);
        if (it != all_rafts_.end()) {
            r = it->second;
            r->Stop();
            all_rafts_.erase(it);
        } else {
            auto it_cr = creating_rafts_.find(id);
            if (it_cr != creating_rafts_.end()) { // in creating
                return Status(Status::kBusy, "remove raft", "raft is in creating");
            } else {
                return Status(Status::kNotFound, "remove raft", std::to_string(id));
            }
        }
    }

    return Status::OK();
}

Status RaftServerImpl::DestroyRaft(uint64_t id, bool backup) {
    FLOG_WARN("destory raft[{}]. backup={}", id, backup);

    std::shared_ptr<RaftImpl> r;
    {
        std::unique_lock<jimkv::shared_mutex> lock(rafts_mu_);
        auto it = all_rafts_.find(id);
        if (it != all_rafts_.end()) {
            r = it->second;
            r->Stop();
            all_rafts_.erase(it);
        } else {
            auto it_cr = creating_rafts_.find(id);
            if (it_cr != creating_rafts_.end()) { // in creating
                return Status(Status::kBusy, "destory raft", "raft is in creating");
            } else {
                return Status(Status::kNotFound, "destory raft", std::to_string(id));
            }
        }
    }

    if (r) {
        auto s = r->Destroy(backup);
        if (!s.ok()) {
            return Status(Status::kIOError, "destroy raft log", s.ToString());
        }
    }
    return Status::OK();
}

std::shared_ptr<RaftImpl> RaftServerImpl::findRaft(uint64_t id) const {
    jimkv::shared_lock<jimkv::shared_mutex> lock(rafts_mu_);

    auto it = all_rafts_.find(id);
    if (it != all_rafts_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

std::shared_ptr<Raft> RaftServerImpl::FindRaft(uint64_t id) const {
    return std::static_pointer_cast<Raft>(findRaft(id));
}

size_t RaftServerImpl::raftSize() const {
    jimkv::shared_lock<jimkv::shared_mutex> lock(rafts_mu_);
    return all_rafts_.size();
}

void RaftServerImpl::GetStatus(ServerStatus* status) const {
    status->total_snap_sending = snapshot_manager_->SendingCount();
    status->total_snap_applying = snapshot_manager_->ApplyingCount();
    status->total_rafts_count  = raftSize();
}

void RaftServerImpl::PostToAllApplyThreads(const std::function<void()>& task) {
    for (auto t: consensus_threads_) {
        auto task_copy = task;
        t->tryPost(std::move(task_copy));
    }
}

Status RaftServerImpl::SetOptions(const std::map<std::string, std::string>& options) {
    return mutable_ops_.Set(options);
}

void RaftServerImpl::onMessage(MessagePtr& msg) {
    if (running_) {
        switch (msg->type()) {
            case pb::HEARTBEAT_REQUEST:
                onHeartbeatReq(msg);
                break;
            case pb::HEARTBEAT_RESPONSE:
                onHeartbeatResp(msg);
                break;
            default: {
                auto raft = findRaft(msg->id());
                if (raft) {
                    raft->RecvMsg(msg);
                }
                break;
            }
        }
    }
}

void RaftServerImpl::onHeartbeatReq(MessagePtr& msg) {
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::HEARTBEAT_RESPONSE);
    resp->set_from(ops_.node_id);
    resp->set_to(msg->from());

    const auto& ids = msg->hb_ctx().ids();
    for (auto it = ids.begin(); it != ids.end(); ++it) {
        uint64_t id = *it;
        auto raft = findRaft(id);
        if (raft) {
            resp->mutable_hb_ctx()->add_ids(id);
            MessagePtr sub_msg(new pb::Message);
            sub_msg->set_id(id);
            sub_msg->set_type(msg->type());
            sub_msg->set_from(msg->from());
            sub_msg->set_to(msg->to());
            raft->RecvMsg(sub_msg);
        }
    }

    transport_->SendMessage(resp);
}

void RaftServerImpl::onHeartbeatResp(MessagePtr& msg) {
    const auto& ids = msg->hb_ctx().ids();
    for (auto it = ids.begin(); it != ids.end(); ++it) {
        uint64_t id = *it;
        auto raft = findRaft(id);
        if (raft) {
            MessagePtr sub_msg(new pb::Message);
            sub_msg->set_id(id);
            sub_msg->set_type(msg->type());
            sub_msg->set_from(msg->from());
            sub_msg->set_to(msg->to());
            raft->RecvMsg(sub_msg);
        }
    }
}

void RaftServerImpl::sendHeartbeat(const RaftMapType& rafts) {
    std::map<uint64_t, std::set<uint64_t>> ctxs;

    for (auto& kv : rafts) {
        auto& r = kv.second;
        if (r->IsLeader()) {
            std::vector<Peer> peers;
            r->GetPeers(&peers);
            for (auto& p : peers) {
                if (p.node_id == ops_.node_id) {
                    continue;
                }
                ctxs[p.node_id].insert(kv.first);
            }
        }
    }

    for (auto& kv : ctxs) {
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::HEARTBEAT_REQUEST);
        msg->set_to(kv.first);
        msg->set_from(ops_.node_id);
        for (auto id : kv.second) {
            msg->mutable_hb_ctx()->add_ids(id);
        }
        transport_->SendMessage(msg);
    }
}

void RaftServerImpl::stepTick(const RaftMapType& rafts) {
    assert(tick_msg_->type() == pb::LOCAL_MSG_TICK);
    for (auto& r : rafts) {
        r.second->Tick(tick_msg_);
    }
}

void RaftServerImpl::tickRoutine() {
    while (running_) {
        std::this_thread::sleep_for(ops_.tick_interval);

        RaftMapType rafts;
        {
            std::lock_guard<jimkv::shared_mutex> lock(rafts_mu_);
            rafts = all_rafts_;
        }
        sendHeartbeat(rafts);
        stepTick(rafts);
        printMetrics();
    }
}

void RaftServerImpl::printMetrics() {
    static time_t last = time(NULL);
    time_t now = time(NULL);
    if (now > last && now - last > 10) {
        last = now;

        // print consensus queue size
        std::string consensus_metrics = "[";
        for (size_t i = 0; i < consensus_threads_.size(); ++i) {
            consensus_metrics += std::to_string(consensus_threads_[i]->size());
            if (i != consensus_threads_.size() - 1) {
                consensus_metrics += ", ";
            }
        }
        consensus_metrics += "]";
        FLOG_INFO("raft[metric] consensus queue size: {}", consensus_metrics);
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
