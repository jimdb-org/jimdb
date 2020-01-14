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

#include "range.h"

#include "base/util.h"
#include "range_logger.h"

namespace jim {
namespace ds {
namespace range {

// handle kv get
void Range::kvGet(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("KvGet begin");

    ErrorPtr err;

    dspb::RangeResponse resp;
    do {
        auto& get_req = req.kv_get();

        if (!verifyKeyInBound(get_req.key(), &err)) {
            break;
        }
        if (ds_config.raft_config.read_option == jim::raft::READ_UNSAFE) {
            auto get_resp = resp.mutable_kv_get();
            auto btime = NowMicros();
            auto ret = store_->Get(get_req.key(), get_resp->mutable_value());
            context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
            get_resp->set_code(static_cast<int>(ret.code()));
        } else {
            submitCmd(std::move(rpc), *req.mutable_header(), jim::raft::READ_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvGet);
                      cmd.set_allocated_kv_get_req(req.release_kv_get());
                  });
            return;
        }
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("KvGet error: {}", err->ShortDebugString());
    }
    sendResponse(rpc, req.header(), resp, std::move(err));
}

// handle kv put
void Range::kvPut(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("KvPut begin");

    ErrorPtr err;
    do {
        if (!verifyKeyInBound(req.kv_put().key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kSoft, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(), jim::raft::WRITE_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvPut);
                      cmd.set_allocated_kv_put_req(req.release_kv_put());
                  });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("KvPut error: {}", err->ShortDebugString());
        dspb::RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyKvPut(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;

    RLOG_DEBUG("ApplyKvPut begin");
    auto &req = cmd.kv_put_req();
    auto btime = NowMicros();

    ErrorPtr err;
    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.key(), &err)) {
            RLOG_WARN("ApplyKvPut failed, epoch is changed");
            ret = Status(Status::kInvalidArgument, "key not int range", "");
            break;
        }

        ret = store_->Put(req.key(), req.value(), raft_index);
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
        if (!ret.ok()) {
            RLOG_ERROR("ApplyKvPut failed, code: {}, msg: {}", ret.code(), ret.ToString());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().size();
            checkSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        dspb::RangeResponse resp;
        resp.mutable_kv_put()->set_code(ret.code());
        replySubmit(cmd, resp, std::move(err), btime);
    }

    return ret;
}

void Range::kvDelete(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("KvDelete begin");

    ErrorPtr err;
    do {
        if (!verifyKeyInBound(req.kv_delete().key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kHard, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(), jim::raft::WRITE_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvDelete);
                      cmd.set_allocated_kv_delete_req(req.release_kv_delete());
                  });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("KvDelete error: {}", err->ShortDebugString());

        dspb::RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyKvDelete(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;
    ErrorPtr err;

    RLOG_DEBUG("ApplyKvDelete begin");

    auto btime = NowMicros();
    auto &req = cmd.kv_delete_req();

    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.key(), &err)) {
            RLOG_WARN("ApplyKvDelete failed, epoch is changed");
            break;
        }

        ret = store_->Delete(req.key(), raft_index);
        context_->Statistics()->PushTime(HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RLOG_ERROR("ApplyKvDelete failed, code: {}, msg: {}",
                       ret.code(), ret.ToString());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        dspb::RangeResponse resp;
        resp.mutable_kv_delete()->set_code(ret.code());
        replySubmit(cmd, resp, std::move(err), btime);
    }

    return ret;
}

void Range::kvDelRange(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("KvDelRange begin");

    ErrorPtr err;
    do {
        if (!verifyKeyInBound(req.kv_del_range().start_key(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.kv_del_range().end_key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kHard, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(), jim::raft::WRITE_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvDelRange);
                      cmd.set_allocated_kv_del_range_req(req.release_kv_del_range());
                  });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("KvDelRang error: {}", err->ShortDebugString());

        dspb::RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyKvDelRange(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;
    ErrorPtr err;

    RLOG_DEBUG("ApplyKvDelRange begin");

    auto btime = NowMicros();
    auto &req = cmd.kv_del_range_req();

    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.start_key(), &err)) {
            RLOG_WARN("ApplyKvDelRange failed, epoch is changed");
            break;
        }

        if (!verifyKeyInBound(req.end_key(), &err)) {
            RLOG_WARN("ApplyKvDelRange failed, epoch is changed");
            break;
        }

        ret = store_->DeleteRange(req.start_key(), req.end_key(), raft_index);
        context_->Statistics()->PushTime(HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RLOG_ERROR("ApplyKvDelRange failed, code: {}, msg: {}",
                       ret.code(), ret.ToString());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        dspb::RangeResponse resp;
        resp.mutable_kv_del_range()->set_code(ret.code());
        replySubmit(cmd, resp, std::move(err), btime);
    }

    return ret;
}

void Range::kvScan(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("KvScan begin");

    ErrorPtr err;

    dspb::RangeResponse resp;
    do {
        auto& scan_req = req.kv_scan();

        if (ds_config.raft_config.read_option == jim::raft::READ_UNSAFE) {
            auto scan_resp = resp.mutable_kv_scan();
            auto btime = NowMicros();

            auto it = store_->NewIterator(scan_req.start_key(), scan_req.end_key());
            while (it->Valid()) {
                auto v = scan_resp->add_values();
                v->set_key(it->Key().c_str(), it->KeySize());
                v->set_value(it->Value().c_str(), it->ValueSize());
                it->Next();
            }

            context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
            scan_resp->set_code(static_cast<int>(Status::Code::kOk));
        } else {
            submitCmd(std::move(rpc), *req.mutable_header(), jim::raft::READ_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvScan);
                      cmd.set_allocated_kv_scan_req(req.release_kv_scan());
                  });
            return;
        }
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("KvScan error: {}", err->ShortDebugString());
    }
    sendResponse(rpc, req.header(), resp, std::move(err));
}

}  // namespace range
}  // namespace ds
}  // namespace jim
