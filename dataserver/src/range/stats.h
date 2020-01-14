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

#include "common/statistics.h"

namespace jim {
namespace ds {
namespace range {

class RangeStats {
public:
    RangeStats() = default;
    virtual ~RangeStats() = default;

    virtual void PushTime(HistogramType type, int64_t time) {}
    virtual void IncrSplitCount() {}
    virtual void DecrSplitCount() {}

    virtual void ReportLeader(uint64_t range_id, bool is_leader) {}
};

}  // namespace range
}  // namespace ds
}  // namespace jim
