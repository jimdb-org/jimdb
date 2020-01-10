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

#include <cstdint>
#include <atomic>
#include <string>

#include "common/rpc_request.h"

#include "storage/store.h"
#include "raft/raft.h"
#include "raft/statemachine.h"
#include "raft/types.h"

#include "mspb/mspb.pb.h"
#include "dspb/raft_internal.pb.h"
#include "dspb/schedule.pb.h"

#include "options.h"
#include "context.h"
#include "meta_keeper.h"
#include "submit.h"

// for test friend class
namespace jimkv { namespace test { namespace helper { class RangeTestFixture; }}}

namespace jimkv {
namespace ds {
namespace range {

class Range : public raft::StateMachine, public std::enable_shared_from_this<Range> {
public:
    Range(const RangeOptions& opt, RangeContext* context, const basepb::Range &meta);
    ~Range();

    Range(const Range &) = delete;
    Range &operator=(const Range &) = delete;

    // if db == nullptr will create one by default
    Status Initialize(std::unique_ptr<db::DB> db = nullptr, uint64_t leader = 0);

    Status Shutdown();

    // Shutdown and delete data
    Status Destroy();

    // Handle request
    void Dispatch(RPCRequestPtr rpc_request, dspb::RangeRequest& request);

    // from raft::StateMachine
    Status Apply(const std::string &cmd, uint64_t index) override;
    Status ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) override;
    Status ApplyReadIndex(const std::string& cmd, uint16_t verify_result) override;
    uint64_t PersistApplied() override;
    void OnReplicateError(const std::string &cmd, const Status &status) override;
    void OnLeaderChange(uint64_t leader, uint64_t term) override;
    std::shared_ptr<raft::Snapshot> GetSnapshot() override;
    Status ApplySnapshotStart(const std::string &context, uint64_t raft_index) override;
    Status ApplySnapshotData(const std::vector<std::string> &datas) override;
    Status ApplySnapshotFinish(uint64_t index) override;

    bool Valid() { return valid_; }
    basepb::Range GetMeta() const { return meta_.Get(); }
    uint64_t GetSplitRangeID() const { return split_range_id_; }
    size_t GetSubmitQueueSize() const { return submit_queue_.Size(); }
    void GetRangeInfo(dspb::RangeInfo *rangeInfo);
    void GetReplica(basepb::Replica *rep);
    void GetPeerInfo(raft::RaftStatus *raft_status);
    uint64_t GetPeerID() const;

    void TransferLeader();
    void AddPeer(const basepb::Peer &peer);
    void DelPeer(const basepb::Peer &peer);

    void ResetStatisSize();
    void ResetStatisSize(uint64_t split_size, uint64_t max_size);
    void SetRealSize(uint64_t rsize) { real_size_ = rsize; }
    void SetKvCount(uint64_t kv_count) {
        kv_count_ = kv_count;
        store_->SetKvCount(kv_count);
    }
    uint64_t GetKvCount() { return kv_count_; }
    void SetStatsInfo( const uint64_t kv_count, const uint64_t real_size);

    // create a new range for split
    Status Split(const dspb::SplitCommand& req, uint64_t raft_index,
            std::shared_ptr<Range>& new_range);

    Status ForceSplit(uint64_t version, std::string* split_key);

    bool VerifyEpoch(const basepb::RangeEpoch& epoch, ErrorPtr& err);

private:
    static std::string raftLogPath(uint64_t table_id, uint64_t range_id);

    Status startRaft(uint64_t leader = 0);

    // kv funcs
    void kvGet(RPCRequestPtr rpc_request, dspb::RangeRequest& req);
    void kvPut(RPCRequestPtr rpc_request, dspb::RangeRequest& req);
    void kvDelete(RPCRequestPtr rpc, dspb::RangeRequest &req);
    void kvDelRange(RPCRequestPtr rpc, dspb::RangeRequest &req);
    void kvScan(RPCRequestPtr rpc, dspb::RangeRequest &req);

    // txn & sql funcs
    void txnPrepare(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnDecide(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnClearup(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnGetLockInfo(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnSelect(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnScan(RPCRequestPtr rpc, dspb::RangeRequest& req);
    void txnSelectFlow(RPCRequestPtr rpc, dspb::RangeRequest& req);

    void idxStatsGet(RPCRequestPtr rpc, dspb::RangeRequest &req);
    void colsStatsGet(RPCRequestPtr rpc, dspb::RangeRequest &req);
    void sendResponse(RPCRequestPtr& rpc, const dspb::RangeRequest_Header& req_header,
                      dspb::RangeResponse& resp, ErrorPtr ptr = nullptr);

    Status submit(const dspb::Command &cmd, uint16_t rw_flag);
    void submitCmd(RPCRequestPtr rpc, dspb::RangeRequest_Header& header, uint16_t rw_flag,
                   const std::function<void(dspb::Command &cmd)> &init);
    void replySubmit(const dspb::Command& cmd, dspb::RangeResponse& resp,
                     ErrorPtr err, int64_t apply_time);
    void clearExpiredContext();

    Status apply(const dspb::Command &cmd, uint64_t index);
    Status applyKvPut(const dspb::Command &cmd, uint64_t raft_index);
    Status applyKvDelete(const dspb::Command &cmd, uint64_t raft_index);
    Status applyKvDelRange(const dspb::Command &cmd, uint64_t raft_index);

    Status applyTxnPrepare(const dspb::Command &cmd, uint64_t raft_index);
    Status applyTxnDecide(const dspb::Command &cmd, uint64_t raft_index);
    Status applyTxnClearup(const dspb::Command &cmd, uint64_t raft_index);
    Status applyAddPeer(const raft::ConfChange &cc, bool *updated);
    Status applyDelPeer(const raft::ConfChange &cc, bool *updated);
    Status applyPromotePeer(const raft::ConfChange &cc, bool *updated);
    Status applySplit(const dspb::Command &cmd, uint64_t index);
    Status applyUpdateStats(const dspb::Command &cmd, uint64_t index);

    void checkSplit(uint64_t size);
    void askSplit(std::string &&key, basepb::Range&& meta, dspb::SplitCommand_StatsInfo &&info, bool force = false);
    void startSplit(mspb::AskSplitResponse &resp, dspb::SplitCommand_StatsInfo &&info);
    void updateStats(const uint64_t total_size, const uint64_t kv_count);

    Status saveMeta(const basepb::Range &meta);

    void scheduleHeartbeat(bool delay);
    void doHeartbeat();
    friend class HeartbeatTimer;

private:
    enum class LimitType { kSoft, kHard };

    // verify and errors functions
    static ErrorPtr newRangeNotFoundErr(uint64_t range_id);
    static ErrorPtr raftFailError();

    basepb::Range getRouteInfo() const;
    ErrorPtr staleEpochErr();
    bool verifyEpoch(const basepb::RangeEpoch& epoch, ErrorPtr* err = nullptr);

    ErrorPtr noLeaderError();
    ErrorPtr notLeaderError(basepb::Peer &&peer);
    bool verifyLeader(ErrorPtr* err = nullptr);

    bool hasSpaceLeft(LimitType type, ErrorPtr* err = nullptr);

    ErrorPtr keyOutOfBoundErr(const std::string &key);
    bool verifyKeyInBound(const std::string &key, ErrorPtr* err = nullptr);
    bool verifyKeyInBound(const dspb::PrepareRequest& req, ErrorPtr* err = nullptr);
    bool verifyKeyInBound(const dspb::DecideRequest& req, ErrorPtr* err = nullptr);

private:
    friend class ::jimkv::test::helper::RangeTestFixture;

    RangeOptions opt_;
    RangeContext* const context_ = nullptr;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    // cache range's start key
    // since it will not change unless we have merge operation
    const std::string start_key_;

    MetaKeeper meta_;

    std::atomic<bool> recovering_ = {true};
    std::atomic<bool> valid_ = { true };

    uint64_t apply_index_ = 0;
    std::atomic<bool> is_leader_ = {false};

    uint64_t real_size_ = 0;
    uint64_t kv_count_ = 0;

    std::atomic<bool> statis_flag_ = {false};
    std::atomic<uint64_t> statis_size_ = {0};
    uint64_t split_range_id_ = 0;

    SubmitQueue submit_queue_;

    std::unique_ptr<storage::Store> store_;
    std::shared_ptr<raft::Raft> raft_;
};

}  // namespace range
}  // namespace ds
}  // namespace jimkv
