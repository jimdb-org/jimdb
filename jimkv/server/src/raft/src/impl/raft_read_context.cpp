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

#include "raft_read_context.h"
#include "common/logger.h"
#include "raft_types.h"
#include "raft_fsm.h"
#include "raft_exception.h"
namespace jimkv {
namespace raft {
namespace impl {

ReadIndexState::ReadIndexState(const uint64_t seq, const uint64_t commit_index, EntryPtr entry)
    :seq_(seq), commit_index_(commit_index), entry_(entry) {

}

uint64_t ReadIndexState::CommitIndex() const {
    return commit_index_;
}

uint64_t ReadIndexState::Seq() const {
    return seq_;
}

EntryPtr ReadIndexState::Entry() {
    return entry_;
}

void ReadIndexState::SetVerify() {
    verify_ = true;
}

bool ReadIndexState::Verify() {
    return verify_;
}
//
uint16_t ReadIndexState::VerifyResult(int repl_num) const {
    int success = 0;
    FLOG_DEBUG("repl_num: {}, commit index: {}, seq: {}, match size: {}", repl_num, commit_index_, seq_, matches_.size());

    if (verify_) {
        return jimkv::raft::READ_SUCCESS;
    }
    for (auto it = matches_.begin(); it != matches_.end(); it++) {
        if (it->second == true) {
            success++;
        }
    }
    if (static_cast<int32_t>(matches_.size()) <= repl_num/2) {
        return READ_WAIT;
    }
    if (success >= (repl_num/2 + 1)) {
        return READ_SUCCESS;
    }
    return READ_FAILURE;
}
void ReadIndexState::SetVerifyResult(uint64_t node_id, bool result) {
    matches_.emplace(std::make_pair(node_id, result));
}
ReadIndexContext::ReadIndexContext(RaftFsm* fsm): fsm_(fsm) {}

bool ReadIndexContext::AddReadIndex(const uint64_t seq, const uint64_t commit_index, EntryPtr entry) {
    reqs_.emplace(std::make_pair(seq, std::make_shared<ReadIndexState>(seq, commit_index, entry)));
    seqs_.push_back(seq);
    if (commit_index > max_commit_index_) {
        max_commit_index_ = commit_index;
    }
    last_seq_ = seq;
    return true;
}

std::shared_ptr<ReadIndexState> ReadIndexContext::GetReadIndexState(const uint64_t seq) {
    auto it = reqs_.find(seq);
    if (it != reqs_.end()) {
        return it->second;
    }
    return nullptr;
}

uint64_t ReadIndexContext::LastSeq() const {
    return last_seq_;
}

void ReadIndexContext::ProcessByApplyIndex(const uint64_t apply_index, int repl_num) {
    uint64_t seq;
    do  {
        if (ReadRequestSize() == 0) {
            FLOG_DEBUG("there is no pending read request");
            return;
        }
        seq = seqs_.front();
        auto read_state = GetReadIndexState(seq);
        if (read_state == nullptr) {
            FLOG_WARN("critical error, not find read state: {}", seq);
            seqs_.pop_front();
        } else {
            if (read_state->CommitIndex() <= apply_index) {//process read
                uint16_t check_result = read_state->VerifyResult(repl_num);
                if (check_result == jimkv::raft::READ_SUCCESS) {
                    auto s = fsm_->ReadProcess(read_state->Entry(), jimkv::raft::READ_SUCCESS);
                    if (!s.ok()) {
                        //todo implements
                        throw RaftException(std::string("statemachine read entry[") +
                                            std::to_string(read_state->Entry()->index()) + "] error: " + s.ToString());
                    }
                    seqs_.pop_front();
                    reqs_.erase(seq);
                } else if (check_result == jimkv::raft::READ_FAILURE) {
                    //todo process failure verify
                    FLOG_WARN("verify failure");
                    auto s = fsm_->ReadProcess(read_state->Entry(), check_result);
                    if (!s.ok()) {
                        //todo implements
                        throw RaftException(std::string("statemachine read entry[") +
                                            std::to_string(read_state->Entry()->index()) + "] error: " + s.ToString());
                    }
                    seqs_.pop_front();
                    reqs_.erase(seq);
                } else {
                    FLOG_DEBUG("seq: {} verify is not finish, please wait", read_state->Seq());
                    return;
                }
            } else {
                break;
            }
        }
    } while(true);
    return;
}

void ReadIndexContext::SetVerifyResult(const uint64_t node_id, const uint64_t seq, bool result) {
    auto read_state = GetReadIndexState(seq);
    if (read_state != nullptr) {
        read_state->SetVerifyResult(node_id, result);
    }
}

void ReadIndexContext::savePendingRequest(std::deque<uint64_t>& pending_seqs,
                                          std::map<uint64_t, std::shared_ptr<ReadIndexState> >& pending_map)
{
    pending_seqs.swap(seqs_);
    pending_map.swap(reqs_);
}

void ReadIndexContext::ProcessBySequence(const uint64_t sequence, uint64_t apply_index, int repl_num) {
    uint64_t first;
    uint16_t verify_result;
    std::shared_ptr<ReadIndexState> read_state = nullptr;
    std::shared_ptr<ReadIndexState> it_read_state = nullptr;
    std::deque<uint64_t> tmp_sequence_queue;
    read_state = GetReadIndexState(sequence);
    if (read_state != nullptr) {
        verify_result = read_state->VerifyResult(repl_num);
        if (verify_result == jimkv::raft::READ_WAIT) {
            FLOG_DEBUG("sequence: {}, apply_index: {}, repl_num: {} verfiy is"
                       " not finish please wait", sequence, apply_index, repl_num);
            return;
        }
    } else {
        FLOG_DEBUG("sequence: {}, apply_index: {}, repl_num: {} read state"
                   " is not exsist, maybe processed", sequence, apply_index, repl_num);
        return;
    }

    for (auto it = seqs_.cbegin(); it != seqs_.cend(); it++) {
        first = *it;
        if (first != sequence) {
            it_read_state = GetReadIndexState(first);
            if (it_read_state != nullptr) {
                if (verify_result == jimkv::raft::READ_SUCCESS) {
                    if (it_read_state->CommitIndex() <= apply_index) { //process it
                        auto s = fsm_->ReadProcess(it_read_state->Entry(), verify_result);
                        if (!s.ok()) {
                            //todo implements
                            throw RaftException(std::string("statemachine read entry[") +
                                                std::to_string(it_read_state->Entry()->index()) + "] error: " + s.ToString());
                        }
                        reqs_.erase(first);
                        seqs_.pop_front();
                    } else {
                        it_read_state->SetVerify();
                    }
                } else {
                    auto s = fsm_->ReadProcess(it_read_state->Entry(), verify_result);
                    if (!s.ok()) {
                        //todo implements
                        throw RaftException(std::string("statemachine read entry[") +
                                            std::to_string(it_read_state->Entry()->index()) + "] error: " + s.ToString());
                    }
                    reqs_.erase(first);
                    seqs_.pop_front();
                }
            }
        } else {
            if (verify_result == jimkv::raft::READ_SUCCESS) {
                if (read_state->CommitIndex() <= apply_index) { //process it
                    auto s = fsm_->ReadProcess(read_state->Entry(), verify_result);
                    if (!s.ok()) {
                        //todo implements
                        throw RaftException(std::string("statemachine read entry[") +
                                            std::to_string(read_state->Entry()->index()) + "] error: " + s.ToString());
                    }
                    reqs_.erase(first);
                    seqs_.pop_front();
                } else {
                    read_state->SetVerify();
                }
            } else {
                auto s = fsm_->ReadProcess(read_state->Entry(), verify_result);
                if (!s.ok()) {
                    //todo implements
                    throw RaftException(std::string("statemachine read entry[") +
                                        std::to_string(read_state->Entry()->index()) + "] error: " + s.ToString());
                }
                reqs_.erase(first);
                seqs_.pop_front();
            }
            break;
        }
    }

    return;
}
uint64_t ReadIndexContext::ReadRequestSize() {
    return reqs_.size();
}

uint64_t ReadIndexContext::MaxCommitIndex() {
    return max_commit_index_;
}

} /* namespace impl*/
} /* namespace raft*/
} /* namespace jimkv*/