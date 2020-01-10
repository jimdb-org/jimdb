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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>

#include "test_util.h"
#include "base/util.h"

#include "raft/snapshot.h"
#include "raft/statemachine.h"
#include "raft/src/impl/snapshot/apply_task.h"
#include "raft/src/impl/snapshot/send_task.h"
#include "raft/src/impl/transport/inprocess_transport.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::raft;
using namespace jimkv::raft::impl;

class TestSnapshot : public Snapshot {
public:
    TestSnapshot() :
        applied_(static_cast<uint64_t>(randomInt())),
        count_(static_cast<uint64_t>(100 + randomInt() % 100)),
        context_(randomString(static_cast<size_t>(100 + randomInt() % 100)))
    {
    }

    uint64_t ApplyIndex() override { return applied_; }
    uint64_t GetCount() const { return count_; }
    const std::string& GetContext() const { return context_; }

    Status Next(std::string* data, bool* over) override {
        if (index_ > count_) {
            *over = true;
        } else {
            *over = false;
            *data = std::to_string(index_++);
        }
        return Status::OK();
    };

    Status Context(std::string* context) override {
        *context = context_;
        return Status::OK();
    }

    void Close() override {}

private:
    const uint64_t applied_ = 0;
    const uint64_t count_ = 0;
    const std::string context_;
    uint64_t index_ = 1;
};

class TestStateMachine : public raft::StateMachine {
public:
    explicit TestStateMachine(std::shared_ptr<TestSnapshot> expected) :
        expected_(std::move(expected)) {
    }

    Status Apply(const std::string& cmd, uint64_t index) override  {
        return Status(Status::kNotSupported);
    }
    Status ApplyMemberChange(const ConfChange& cc, uint64_t index) override {
        return Status(Status::kNotSupported);
    }
    void OnReplicateError(const std::string& cmd, const Status& status) override  {}
    void OnLeaderChange(uint64_t leader, uint64_t term) override {}
    std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }

    Status ApplySnapshotStart(const std::string& context, uint64_t index) override {
        if (context != expected_->GetContext()) {
            return Status(Status::kInvalid, "snapshot context",
                          context + " != " + expected_->GetContext());
        }
        return Status::OK();
    }

    uint64_t PersistApplied() override {
        return 0;
    }

    Status ApplySnapshotData(const std::vector<std::string>& datas) override {
        for (const auto& data: datas) {
            auto i = std::strtoul(data.c_str(), NULL, 10);
            if (i != pre_num_ + 1) {
                return Status(Status::kInvalid, "snapshot data",
                              std::to_string(pre_num_) + "+1 != " + data);
            }
            ++pre_num_;
        }
        return Status::OK();
    }

    Status ApplySnapshotFinish(uint64_t index) override {
        Status s;
        if (pre_num_ != expected_->GetCount()) {
            s = Status(Status::kInvalid, "snapshot count",
                          std::to_string(pre_num_) + " != "  + std::to_string(expected_->GetCount()));
        } else if (index != expected_->ApplyIndex()) {
            s = Status(Status::kInvalid, "snapshot index",
                          std::to_string(index) + " != "  + std::to_string(expected_->ApplyIndex()));

        }
        return s;
    }

    Status ApplyReadIndex(const std::string& cmd, uint16_t verify_result) override {
        return Status(Status::kNotSupported);
    }

private:
    std::shared_ptr<TestSnapshot> expected_;
    uint64_t pre_num_ = 0;
};


TEST(Snapshot, SendAndApply) {
    const uint64_t kSendNodeID = 1;
    const uint64_t kApplyNodeID = 2;
    const uint64_t kSnapTerm = static_cast<uint64_t>(randomInt());
    const uint64_t kSnapUUID = static_cast<uint64_t>(randomInt());
    const uint64_t kRaftID = static_cast<uint64_t>(randomInt());

    bool send_finished = false, apply_finished = false;
    uint64_t send_blocks = {0}, apply_blocks = {0};
    uint64_t send_bytes = {0}, apply_bytes = {0};
    std::mutex mu;
    std::condition_variable cond;

    auto snap = std::make_shared<TestSnapshot>();

    // prepare apply task
    SnapContext apply_ctx;
    apply_ctx.to = kApplyNodeID;
    apply_ctx.from = kSendNodeID;
    apply_ctx.uuid = kSnapUUID;
    apply_ctx.id = kRaftID;
    apply_ctx.term = kSnapTerm;

    auto sm = std::make_shared<TestStateMachine>(snap);
    auto receiver = std::make_shared<ApplySnapTask>(apply_ctx, sm);
    receiver->SetOptions(ApplySnapTask::Options());

    auto apply_trans = new transport::InProcessTransport(kApplyNodeID);
    auto s = apply_trans->Start("", 1234, [=](MessagePtr& msg) {
        auto s = receiver->RecvData(msg);
        ASSERT_TRUE(s.ok()) << "Recv error: " << s.ToString();
    });
    ASSERT_TRUE(s.ok()) << s.ToString();
    receiver->SetTransport(apply_trans);

    receiver->SetReporter([&](const SnapContext& ctx, const SnapResult& result) {
        auto s = testutil::Equal(ctx, apply_ctx);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(result.status.ok()) << result.status.ToString();
        std::cout << "apply bytes: " << result.bytes_count <<
                  ", blocks: " << result.blocks_count << std::endl;
        ASSERT_GT(result.bytes_count, 0U);
        ASSERT_GT(result.blocks_count, 0U);

        // notify apply finished
        {
            std::lock_guard<std::mutex> lock(mu);
            apply_blocks = result.blocks_count;
            apply_bytes = result.bytes_count;
            apply_finished = true;
            cond.notify_one();
        }
    });

    // prepare send task
    SnapContext send_ctx;
    send_ctx.to = kApplyNodeID;
    send_ctx.from = kSendNodeID;
    send_ctx.id = kRaftID;
    send_ctx.term = kSnapTerm;
    send_ctx.uuid = kSnapUUID;

    pb::SnapshotMeta meta;
    meta.set_index(snap->ApplyIndex());
    meta.set_term(kSnapTerm);
    meta.set_context(snap->GetContext());

    auto sender = std::make_shared<SendSnapTask>(send_ctx, std::move(meta), snap);
    SendSnapTask::Options sops;
    sops.max_size_per_msg = 10;
    sender->SetOptions(sops);

    auto send_trans = new transport::InProcessTransport(kSendNodeID);
    s = send_trans->Start("", 1234, [=](MessagePtr& msg) {
        auto s = sender->RecvAck(msg);
        ASSERT_TRUE(s.ok()) << "Recv error: " << s.ToString();
    });
    sender->SetTransport(send_trans);

    sender->SetReporter([&](const SnapContext& ctx, const SnapResult& result) {
        auto s = testutil::Equal(ctx, send_ctx);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(result.status.ok()) << result.status.ToString();
        ASSERT_GT(result.bytes_count, 0U);
        ASSERT_GT(result.blocks_count, 0U);
        std::cout << "send bytes: " << result.bytes_count <<
                  ", blocks: " << result.blocks_count << std::endl;

        // notify send finished
        {
            std::lock_guard<std::mutex> lock(mu);
            send_blocks = result.blocks_count;
            send_bytes = result.bytes_count;
            send_finished = true;
            cond.notify_one();
        }
    });

    // start to send
    std::thread send_thr([=] { sender->Run(); });
    send_thr.detach();

    // start to recv and apply
    std::thread apply_thr([=] { receiver->Run(); });
    apply_thr.detach();

    // wait tasks finished
    auto expire = std::chrono::system_clock::now() + std::chrono::seconds(5);
    bool ret = false;
    {
        std::unique_lock<std::mutex> lock(mu);
        ret = cond.wait_until(lock, expire, [&]{ return send_finished && apply_finished; });
    }
    ASSERT_TRUE(ret) << "wait snapshot finsih timeout";

    ASSERT_EQ(send_blocks, apply_blocks);
    ASSERT_EQ(send_bytes, apply_bytes);

    delete apply_trans;
    delete send_trans;
}

}  // namespace
