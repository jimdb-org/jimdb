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
_Pragma("once");

#include <map>
#include <deque>
#include <memory>
#include "raft_types.h"
namespace jimkv {
namespace raft {
namespace impl {
class RaftFsm;
class ReadIndexState {
public:
    ReadIndexState(const uint64_t seq, const uint64_t commit_index, EntryPtr entry);
    ~ReadIndexState() = default;

    ReadIndexState(const ReadIndexState&) = delete;
    ReadIndexState& operator=(const ReadIndexState&) = delete;

    uint64_t CommitIndex() const;

    uint64_t Seq() const;

    EntryPtr Entry();

    uint16_t VerifyResult(int repl_num) const;
    void SetVerifyResult(uint64_t node_id, bool result);
    void SetVerify();
    bool Verify();
private:
    uint64_t seq_; //唯一id
    uint64_t commit_index_; //current commit index
    EntryPtr entry_;
    bool     verify_ = false;
    std::map<uint64_t, bool> matches_; //确认正确的数量,key: node_id
};

class ReadIndexContext {
public:
    ReadIndexContext(RaftFsm *fsm);
    ~ReadIndexContext() = default;

    ReadIndexContext(const ReadIndexContext&) = delete;
    ReadIndexContext& operator=(const ReadIndexContext&) = delete;
    bool AddReadIndex(const uint64_t seq, const uint64_t commit_index, EntryPtr entry);

    std::shared_ptr<ReadIndexState> GetReadIndexState(const uint64_t seq);

    void ProcessByApplyIndex(const uint64_t apply_index, int repl_num);

    void ProcessBySequence(const uint64_t sequence, const uint64_t apply_index, int repl_num);

    void SetVerifyResult(const uint64_t node_id, const uint64_t seq, bool result);

    uint64_t LastSeq() const;

    uint64_t ReadRequestSize();

    uint64_t MaxCommitIndex();
    void savePendingRequest(std::deque<uint64_t>& pending_seqs, std::map<uint64_t, std::shared_ptr<ReadIndexState> >& pending_map);

private:
    uint64_t last_seq_;
    uint64_t max_commit_index_;
    RaftFsm *fsm_;
    std::map<uint64_t, std::shared_ptr<ReadIndexState> >reqs_;
    std::deque<uint64_t> seqs_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */