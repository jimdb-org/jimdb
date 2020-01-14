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

#include "stats_idx.h"
#include "proto/gen/dspb/processorpb.pb.h"
#include "histogram.h"
#include "cm_sketch.h"

namespace jim {
namespace ds {
namespace storage {

StatsIdx::StatsIdx(const dspb::IndexStatsRequest& req, Store& s)
    :bucketsMax_(req.bucket_max()),sketchDepth_(req.cmsketch_depth())
    ,sketchWidth_(req.cmsketch_width()) {
    dspb::IndexRead indexRead;
    indexRead.mutable_columns()->CopyFrom(req.columns_info());
    indexRead.set_type(dspb::KEYS_RANGE_TYPE);
    indexRead.mutable_range()->CopyFrom(req.range());
    indexRead.set_unique(req.unique());
    indexRead_ = std::unique_ptr<IndexRead>(new IndexRead(indexRead, req.range(), s, false));
}

Status StatsIdx::StatsIdxResp(dspb::IndexStatsResponse& resp) {
    if (bucketsMax_ <= 0 || sketchDepth_ <= 0 || sketchWidth_ <= 0) {
        return Status( Status::kInvalidArgument,
                       "IndexStatsRequest param error!",
                       "bucketsMax:" + std::to_string(bucketsMax_)
                       + "; sketchDepth:" + std::to_string(sketchDepth_)
                       + "; sketchWidth:" + std::to_string(sketchWidth_));

    }

    Histogram hist(bucketsMax_);
    CMSketch cms(sketchDepth_, sketchWidth_);
    Status s;
    auto colIds = indexRead_->get_col_ids();
    uint64_t nullCount = 0;
    do {
        RowResult row;
        s = indexRead_->next(row);
        if (!s.ok()) {
            break;
        }
        hist.Append(row.GetValue());
        auto maps = row.GetMapFields();
        // std::cout << "maps.size:" << maps.size() << std::endl;
        for (auto& entry : maps) {
            // std::cout << "first:" << entry.first << std::endl;
            // std::cout << "second:" << entry.second->ToString() << std::endl;
            if (entry.second && std::find(colIds.begin(), colIds.end(), entry.first) != colIds.end()) {
                cms.Insert(entry.second->ToString());
            }
        }
        if (colIds.size() == 1 && maps.find(colIds[0]) == maps.end()) {
            ++nullCount;
        }
    } while(s.ok());

    hist.ToProto(*(resp.mutable_hist()));
    cms.ToProto(*(resp.mutable_cms()));
    resp.set_null_count(nullCount);

    return Status::OK();     
}
}// end storage
}// end ds
}// end jim
