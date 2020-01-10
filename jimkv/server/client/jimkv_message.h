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

_Pragma ("once");

#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <atomic>
#include <mutex>
#include <chrono>

#include "asio/steady_timer.hpp"
#include "dspb/api.pb.h"

#include "net/message.h"
#include "jimkv_command.h"
#include "jimkv_scope.h"
#include "jimkv_route.h"

namespace jimkv {
namespace sdk {

class JimMessage final : public std::enable_shared_from_this<JimMessage> {
public:
    using MsgPtr = std::shared_ptr<JimMessage>;
    enum State {EXEC, WAIT, END};

public:
    JimMessage(CmdPtr cmd) : cmd_(cmd) {}

    JimMessage(CmdPtr cmd, JimScope& scope) :
        start_key_(std::move(scope.start())),
        end_key_(std::move(scope.end())),
        cmd_(cmd)
    {}

    ~JimMessage() = default;

    JimMessage(const JimMessage &) = delete;
    JimMessage& operator=(const JimMessage&) = delete;

    uint64_t msg_id() {return msg_id_;}
    CmdPtr cmd() {return cmd_;}
    const net::Message& msg() {return msg_;}

    const std::string& start_key() {return start_key_;}
    const std::string& end_key() {return end_key_;}

    bool has_parent() {return parent_ != nullptr;}
    void parent_set(MsgPtr msg) {parent_ = msg;}
    MsgPtr parent() {return parent_;}

    bool Serialize(RangePtr rng);
    void AddSubMsg(MsgPtr sub_msg);

    void SendBegin() {state_ = State::EXEC;}
    void SendEnd();
    void Complete();

    void expire_at_set(asio::steady_timer::time_point expire) {
        expire_at_ = expire;
    }
    asio::steady_timer::time_point expire_at() {return expire_at_;}

    bool timeout() {
        return expire_at_ <= asio::steady_timer::clock_type::now();
    }

    void timer_set(std::shared_ptr<asio::steady_timer> timer) {timer_ = timer;};

    void TimerCancel() {timer_->cancel();}

    void Timeout();
    void TimeoutHandle(std::function<int()> pred, const asio::error_code& e);
private:

    bool KvGet(dspb::RangeRequest& req);
    bool KvPut(dspb::RangeRequest& req);
    bool KvDelete(dspb::RangeRequest& req);
    bool KvScan(dspb::RangeRequest& req);
    bool KvDelRange(dspb::RangeRequest& req);

    void do_complete();
private:
    uint64_t msg_id_;

    std::string start_key_;
    std::string end_key_;

    CmdPtr cmd_;
    MsgPtr parent_;

    net::Message msg_;

    asio::steady_timer::time_point expire_at_;
    std::shared_ptr<asio::steady_timer> timer_;

    std::atomic<size_t> sub_msg_completed_ = {0};
    std::atomic<State> state_ = {State::EXEC};

    std::map<uint64_t, MsgPtr> sub_msg_;
    std::mutex sub_msg_mutex_;
};

using MsgPtr = std::shared_ptr<JimMessage>;

} // namespace sdk
} // namespace jimkv

