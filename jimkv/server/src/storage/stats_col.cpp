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

#include "stats_col.h"
#include "processor_table_read.h"
#include "processor_order_by.h"
#include "histogram.h"

namespace jimkv {
namespace ds {
namespace storage {
StatsCol::StatsCol(const dspb::ColumnsStatsRequest& req, Store& s){ 
    keySchema_.CopyFrom(s.GetKeySchema());
    colLen_ = req.columns_info_size();
    for (auto& schemaCol : keySchema_.key_cols()) {
        if (schemaCol.id() == req.columns_info(0).id()) {
            pk_id_ = schemaCol.id();
            --colLen_;
            has_pk_ = true;
            break;
        }
    }
    bucketMax_ = req.bucket_max();
    fmSketchMax_ = req.sketch_max();
    sampleMax_  = req.sample_max();
    cmSketchDepth_ = req.cmsketch_depth();
    cmSketchWidth_ = req.cmsketch_width();
    dspb::TableRead tableRead;
    tableRead.set_type(dspb::KEYS_RANGE_TYPE);
    dspb::Ordering ordering;
    ordering.set_count(10000);
    for (auto& cl : req.columns_info()) {
        tableRead.add_columns()->CopyFrom(cl);
        auto clOrdering = ordering.add_columns();
        clOrdering->mutable_expr()->set_expr_type(dspb::Column);
        clOrdering->mutable_expr()->mutable_column()->set_id(cl.id());
        clOrdering->set_asc(true);
        if (cl.id() != pk_id_) {
            cols_.push_back(cl.id());
        }
    }

    tableRead.mutable_range()->CopyFrom(req.range());
    std::unique_ptr<Processor> processor = std::unique_ptr<Processor>(new TableRead( tableRead, req.range(), s , false));
    processor = std::unique_ptr<Processor>(new OrderBy( ordering, std::move(processor), false));
    processor_ = std::move(processor);
}

Status StatsCol::StatsColResp(dspb::ColumnsStatsResponse& resp){
    Histogram pk_hist(bucketMax_);
    SampleCollector sample(sampleMax_, fmSketchMax_, cmSketchDepth_, cmSketchWidth_);
    std::vector<SampleCollector> collects(colLen_, sample); 
    Status s;
    
    do {
        RowResult row;
        s = processor_->next(row);
        if (!s.ok()) break;
        auto mapFields = row.GetMapFields();
        if (has_pk_) {
            auto pkPair = mapFields.find(pk_id_);
            if (pkPair != mapFields.end() && pkPair->second != nullptr) {
                pk_hist.Append(pkPair->second->ToString());
            }
        }
        
        for (uint32_t n = 0; n < cols_.size(); ++n) {
            auto field = mapFields.find(cols_[n]);
            if (field != mapFields.end()) {
                collects[n].Collect(field->second->ToString().c_str());
            } else {
                collects[n].Collect(nullptr);
            }
        }
    } while(s.ok());

    pk_hist.ToProto(*(resp.mutable_pk_hist()));
    for(auto& collect : collects) {
        collect.ToProto(*(resp.add_collectors()));
    }

    return Status::OK();
}
}// end storage
}// end ds
}// end jimkv
