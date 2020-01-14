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

#include "raft/snapshot.h"
#include "dspb/raft_internal.pb.h"
#include "db/db.h"

namespace jim {
namespace ds {
namespace storage {

using Iter = db::Iterator;
using IterPtr = db::IteratorPtr;

class Snapshot : public raft::Snapshot {
public:
    Snapshot(uint64_t applied, std::string&& context,
            db::IteratorPtr data_iter, db::IteratorPtr txn_iter);

    ~Snapshot();

    Status Context(std::string* context) override;
    uint64_t ApplyIndex() override { return applied_; }

    Status Next(std::string* data, bool* over) override;
    void Close() override;

private:
    Status next(Iter *iter, dspb::CFType type, std::string* data, bool *over);

private:
    uint64_t applied_ = 0;
    std::string context_;
    IterPtr data_iter_;
    IterPtr txn_iter_;
    bool txn_over_ = false;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jim */
