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
//

_Pragma("once");

#include <vector>
#include <memory>
#include <future>
#include <atomic>

#include "asio.hpp"

#include "jimkv_common.h"
#include "jimkv_queue.h"

#include "dspb/kv.pb.h"
#include "dspb/api.pb.h"
#include "dspb/function.pb.h"
#include "dspb/error.pb.h"

namespace jimkv {
namespace sdk {

using RespPtr = std::shared_ptr<dspb::RangeResponse>;

typedef std::function<void (const ArrayVoidPtr &, VoidPtr)> AsyncFn;

class JimCommand {
public:
    JimCommand(uint64_t cluster_id, uint64_t db_id, uint64_t table_id) :
        cluster_id_(cluster_id),
        db_id_(db_id),
        table_id_(table_id)
    {}

    virtual ~JimCommand() = default;

    JimCommand(const JimCommand&) = delete;
    JimCommand& operator=(const JimCommand&) = delete;

    virtual JimStatus encode(const std::vector<std::string>& args) = 0;
    virtual JimStatus append_sub_command(const std::vector<std::string>& args) = 0;

    virtual void net_callback() = 0;
    virtual JimStatus build_reply() = 0;

public:
    using CmdPtr = std::shared_ptr<JimCommand>;

    void set_parent(CmdPtr parent) {parent_ = parent;}
    CmdPtr parent() {return parent_;}

    uint64_t cluster_id() {return cluster_id_;}
    uint64_t db_id() {return db_id_;};
    uint64_t table_id() {return table_id_;};

    uint16_t hash_code() {return hash_code_;}
    void set_hash_code(uint16_t code) {hash_code_ = code;}

    void set_req_type(const dspb::RangeRequest::ReqCase cmd_type) {
        req_type_ = cmd_type;
    }
    dspb::RangeRequest::ReqCase req_type() {return req_type_;};

    void set_callback(AsyncFn callback, VoidPtr user_data) {
        callback_ = callback;
        user_data_ = user_data;
    }
    AsyncFn callback() {return callback_;}
    VoidPtr user_data() {return user_data_;}

    void add_response(RespPtr response) {response_.push_back(response);}
    std::vector<RespPtr>& response() {return response_;}

    void add_sub_command(CmdPtr child_cmd) {sub_commands_.push_back(child_cmd);}
    bool has_sub_command() {return !sub_commands_.empty();}
    void clear_sub_command() {sub_commands_.clear();}

    const std::vector<CmdPtr>& sub_command() {return sub_commands_;}
    const ArrayVoidPtr& reply() {return reply_;};

    void set_encode_key(std::string &encode_key) {encode_key_ = encode_key;}
    const std::string& encode_key() {return encode_key_;};

    void set_encode_value(std::string &encode_value) {encode_value_ = encode_value;}
    const std::string& encode_value() {return encode_value_;};

    void set_rw_flag(CmdRWFlag flag) {rw_flag_ = flag;}
    CmdRWFlag rw_flag() {return rw_flag_;}

    uint64_t sub_commands_count() {return sub_commands_count_;}

    // for parent, return true if sub_commands_count - complete_count == 0,
    // else false && complete_count += accomplish_count
    bool is_finished(uint64_t accomplish_count) {
        auto count = complete_count_.fetch_add(accomplish_count, std::memory_order_relaxed);
        return sub_commands_count_ == count + accomplish_count;
    }
    void finished() {promise_.set_value(true);}
    std::future<bool> get_future() {return promise_.get_future();}

    void set_valid(bool valid) {
        if (parent_ != nullptr) {
            parent_->set_valid(valid);
        } else {
            valid_ = valid;
        }
    }

    bool valid() {
        if (parent_ != nullptr) {
            return parent_->valid();
        } else {
            return valid_;
        }
    }
protected:
    uint64_t cluster_id_;
    uint64_t db_id_;
    uint64_t table_id_;

    dspb::RangeRequest::ReqCase req_type_;

    std::string encode_key_;
    std::string encode_value_;

    uint16_t hash_code_;

    CmdRWFlag rw_flag_ = CmdRWFlag::WRITE;

    //JimQueue<RespPtr> response_;
    std::vector<RespPtr> response_;

    std::promise<bool> promise_;

    CmdPtr parent_;

    std::atomic<bool> valid_ = {true};
    std::atomic<uint64_t> complete_count_ = {0};
    std::atomic<uint64_t> sub_commands_count_ = {0};
    std::vector<CmdPtr> sub_commands_;

    AsyncFn callback_;       //used by async
    VoidPtr user_data_; //used by async
    ArrayVoidPtr reply_;
};

using CmdPtr = std::shared_ptr<JimCommand>;

} //namespace sdk
} //namespace jimkv
