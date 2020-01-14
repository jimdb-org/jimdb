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

#include "raft_log.h"

#include <sstream>
#include "common/logger.h"
#include "raft_exception.h"
#include "raft_log_unstable.h"
#include "storage/storage.h"

namespace jim {
namespace raft {
namespace impl {

RaftLog::RaftLog(uint64_t id, const std::shared_ptr<storage::Storage>& s)
    : id_(id), storage_(s) {
    auto status = open();
    if (!status.ok()) {
        throw RaftException(status);
    }
}

Status RaftLog::open() {
    uint64_t first_index = 0;
    auto s = storage_->FirstIndex(&first_index);
    if (!s.ok()) {
        return Status(Status::kIOError, "load log first index", s.ToString());
    }

    uint64_t last_index = 0;
    s = storage_->LastIndex(&last_index);
    if (!s.ok()) {
        return Status(Status::kIOError, "load log last index", s.ToString());
    }

    unstable_.reset(new UnstableLog(last_index + 1));
    committed_ = first_index - 1;
    applied_ = first_index - 1;

    return Status::OK();
}

uint64_t RaftLog::firstIndex() const {
    uint64_t first_index = 0;
    auto s = storage_->FirstIndex(&first_index);
    if (!s.ok()) {
        throw RaftException(std::string("get first index from raft log error:") +
                            s.ToString());
    }
    return std::max(first_index, storage_->InheritIndex() + 1);
}

uint64_t RaftLog::lastIndex() const {
    uint64_t last_index = 0;
    if (unstable_->maybeLastIndex(&last_index)) {
        return last_index;
    }

    auto s = storage_->LastIndex(&last_index);
    if (!s.ok()) {
        throw RaftException(std::string("get last index from raft log error:") +
                            s.ToString());
    }
    return last_index;
}

Status RaftLog::term(uint64_t index, uint64_t* term) const {
    uint64_t dummy_index = firstIndex() - 1;
    if (index < dummy_index || index > lastIndex()) {
        *term = 0;
        return Status::OK();
    }

    if (unstable_->maybeTerm(index, term)) {
        return Status::OK();
    }

    bool compacted = false;
    Status s = storage_->Term(index, term, &compacted);
    if (!s.ok()) {
        return Status(Status::kIOError, "get raft log term", s.ToString());
    } else if (compacted) {
        *term = 0;
        return Status(Status::kCompacted);
    } else {
        return s;
    }
}

uint64_t RaftLog::lastTerm() const {
    uint64_t term = 0;
    auto s = this->term(lastIndex(), &term);
    if (!s.ok()) {
        throw RaftException(std::string("[raftLog->lastTerm]unexpected error "
                                        "when getting the last term: ") +
                            s.ToString());
    }
    return term;
}

void RaftLog::lastIndexAndTerm(uint64_t* index, uint64_t* term) const {
    *index = lastIndex();
    auto s = this->term(*index, term);
    if (!s.ok()) {
        throw RaftException(
            std::string("[raftLog->lastIndexAndTerm]unexpected error when "
                        "getting the last term: ") +
            s.ToString());
    }
}

bool RaftLog::matchTerm(uint64_t index, uint64_t term) const {
    uint64_t t = 0;
    auto s = this->term(index, &t);
    if (!s.ok()) {
        return false;
    }
    return t == term;
}

uint64_t RaftLog::findConfilct(const std::vector<EntryPtr>& ents) const {
    for (const auto& e : ents) {
        if (!matchTerm(e->index(), e->term())) {
            if (e->index() <= this->lastIndex()) {
                uint64_t eterm = 0;
                auto s = this->term(e->index(), &eterm);
                FLOG_INFO("raft[{}] found conflict at index {} [existing term: {}, conflicting term: {}]",
                         id_, e->index(), this->zeroTermOnErrCompacted(eterm, s), e->term());
            }
            return e->index();
        }
    }
    return 0;
}

bool RaftLog::maybeAppend(uint64_t index, uint64_t log_term, uint64_t commit,
                          const std::vector<EntryPtr>& ents, uint64_t* lastnewi) {
    if (matchTerm(index, log_term)) {
        *lastnewi = index + ents.size();
        uint64_t ci = findConfilct(ents);
        if (ci > 0) {
            if (ci <= committed_) {
                std::ostringstream ss;
                ss << "[raftLog->maybeAppend]entry " << ci
                   << "conflict with committed entry [committed(" << committed_ << "]";
                throw RaftException(ss.str());
            } else {
                auto start = static_cast<int>(ci - (index + 1));
                append(std::vector<EntryPtr>(ents.begin() + start, ents.end()));
            }
        }
        commitTo(std::min(commit, *lastnewi));
        return true;
    }
    return false;
}

uint64_t RaftLog::append(const std::vector<EntryPtr>& ents) {
    if (ents.empty()) {
        return this->lastIndex();
    }
    if (ents[0]->index() - 1 < committed_) {
        std::ostringstream ss;
        ss << "[raftLog->append]after(" << ents[0]->index() - 1
           << ") is out of range [committed(" << committed_ << ")]";
        throw RaftException(ss.str());
    }
    unstable_->truncateAndAppend(ents);
    return lastIndex();
}

void RaftLog::unstableEntries(std::vector<EntryPtr>* ents) { unstable_->entries(ents); }

void RaftLog::nextEntries(uint64_t max_size, std::vector<EntryPtr>* ents) {
    uint64_t lo = std::max(applied_ + 1, firstIndex());
    uint64_t hi = committed_ + 1;
    if (hi > lo) {
        Status s = this->slice(lo, hi, max_size, ents);
        if (!s.ok()) {
            std::ostringstream ss;
            ss << "[raftLog->nextEnts]unexpected error when getting unapplied[" << lo
               << "," << hi << ") entries (%" << s.ToString() << ")";
            throw RaftException(ss.str());
        }
    }
}

Status RaftLog::entries(uint64_t index, uint64_t max_size,
                        std::vector<EntryPtr>* ents) const {
    if (index > lastIndex()) {
        return Status::OK();
    }
    return this->slice(index, lastIndex() + 1, max_size, ents);
}

bool RaftLog::maybeCommit(uint64_t max_index, uint64_t term) {
    if (max_index > committed_) {
        uint64_t t = 0;
        auto s = this->term(max_index, &t);
        if (this->zeroTermOnErrCompacted(t, s) == term) {
            this->commitTo(max_index);
            return true;
        }
    }
    return false;
}

void RaftLog::commitTo(uint64_t commit) {
    if (committed_ < commit) {
        if (lastIndex() < commit) {
            std::ostringstream ss;
            ss << "[raftLog->commitTo]tocommit(" << commit
               << ") is out of range [lastIndex(" << lastIndex() << ")]";
            throw RaftException(ss.str());
        }
        committed_ = commit;
    }
}

void RaftLog::appliedTo(uint64_t index) {
    if (index == 0) {
        return;
    }
    if (committed_ < index || index < applied_) {
        std::ostringstream ss;
        ss << "[raftLog->appliedTo]applied(" << index << ") is out of range [prevApplied("
           << applied_ << "), committed(" << committed_ << ")]";
        throw RaftException(ss.str());
    }
    applied_ = index;
}

void RaftLog::stableTo(uint64_t index, uint64_t term) {
    unstable_->stableTo(index, term);
}

bool RaftLog::isUpdateToDate(uint64_t lasti, uint64_t term) {
    uint64_t li = 0, lt = 0;
    this->lastIndexAndTerm(&li, &lt);
    return term > lt || ((term == lt) && (lasti >= li));
}

void RaftLog::restore(uint64_t index) {
    committed_ = index;
    applied_ = index;
    unstable_->restore(index);
}

void limitSize(std::vector<EntryPtr>& ents, uint64_t max_size) {
    if (ents.empty() || max_size == kNoLimit) {
        return;
    }

    uint64_t size = 0;
    for (auto it = ents.begin(); it != ents.end(); ++it) {
        size += (*it)->ByteSizeLong();
        if (size > max_size) {
            if (it != ents.begin()) {
                ents.erase(it, ents.end());
            }
            break;
        }
    }
}

Status RaftLog::slice(uint64_t lo, uint64_t hi, uint64_t max_size,
                      std::vector<EntryPtr>* ents) const {
    if (lo == hi) {
        return Status::OK();
    }
    Status s = this->mustCheckOutOfBounds(lo, hi);
    if (!s.ok()) {
        return s;
    }

    if (lo < unstable_->offset()) {
        uint64_t index = std::min(hi, unstable_->offset());
        bool compacted = false;
        auto s = storage_->Entries(lo, index, max_size, ents, &compacted);
        if (compacted) {
            return Status(Status::kCompacted);
        } else if (!s.ok()) {
            std::stringstream ss;
            ss << "[raftLog->slice]get entries[" << lo << ":" << index
               << ") from storage err:[" << s.ToString() << "]";
            throw RaftException(ss.str());
        } else {
            if (ents->size() < index - lo) {
                return Status::OK();
            }
        }
    }

    if (hi > unstable_->offset()) {
        unstable_->slice(std::max(lo, unstable_->offset()), hi, ents);
    }

    if (max_size == kNoLimit) {
        return Status::OK();
    }
    limitSize(*ents, max_size);

    return Status::OK();
}

Status RaftLog::mustCheckOutOfBounds(uint64_t lo, uint64_t hi) const {
    if (lo > hi) {
        std::ostringstream ss;
        ss << "[raftLog->mustCheckOutOfBounds]invalid slice " << lo << " > " << hi;
        throw RaftException(ss.str());
    }
    uint64_t fi = this->firstIndex();
    if (lo < fi) {
        return Status(Status::kCompacted);
    }
    uint64_t li = this->lastIndex();
    uint64_t length = li - fi + 1;
    if (lo < fi || hi > fi + length) {
        std::stringstream ss;
        ss << "[raftLog->mustCheckOutOfBounds]slice[" << lo << "," << hi
           << ") out of bound [" << fi << "," << li << "]";
        throw RaftException(ss.str());
    }
    return Status::OK();
}

uint64_t RaftLog::zeroTermOnErrCompacted(uint64_t term, const Status& s) {
    if (s.ok()) {
        return term;
    } else if (s.code() == Status::kCompacted) {
        return 0;
    } else {
        throw RaftException(
            std::string("[raftLog->zeroTermOnErrCompacted]unexpected error:") +
            s.ToString());
    }
}

void RaftLog::allEntries(std::vector<EntryPtr>* ents) const {
    Status s = this->entries(firstIndex(), kNoLimit, ents);
    if (s.ok()) {
        return;
    } else if (s.code() == Status::kCompacted) {
        // try again if there was a racing compaction
        allEntries(ents);
        return;
    } else {
        throw RaftException(std::string("[log->allEntries]get all entries err:") +
                            s.ToString());
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
