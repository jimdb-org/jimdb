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


#include "jimkv_conn.h"

#include "jimkv_client.h"
#include "jimkv_assist.h"
#include "jimkv_log.h"

#include "dspb/api.pb.h"

namespace jimkv {
namespace sdk {

using namespace std::placeholders;

bool JimConn::Connect(std::string &ip, std::string &port) {
    remote_addr_ = ip + ":" + port;

    std::error_code ec;
    asio::ip::tcp::resolver resolver(socket_.get_executor());

    endpoint_ = resolver.resolve(ip, port, ec);
    if (ec) {
        CBLOG_ERROR("resolve %s error: %s",
            remote_addr_.c_str(), ec.message().c_str());
        return false;
    }

    do_connect();
    return true;
}

void JimConn::Close() {
    if (state_ != ConnState::Closed) {
        auto self(shared_from_this());
        asio::post(socket_.get_executor(), [self] { self->do_close(); });
    }
}

void JimConn::do_connect() {
    auto self(shared_from_this());
    asio::async_connect(socket_, endpoint_,
        [this, self](std::error_code ec, asio::ip::tcp::endpoint) {
            if (ec) {
                CBLOG_ERROR("connect to %s error: %s",
                    remote_addr_.c_str(), ec.message().c_str());

                state_ = ConnState::Closing;
                return do_reconnect();
            }

            state_ = ConnState::Established;
            fd_ = socket_.native_handle();
            do_idle();
            // start read
            read_head();
            // start send
            do_send();
        });
}

void JimConn::do_reconnect() {
    if ( state_ == ConnState::Connecting) {
        return;
    }

    do_close();
    state_ = ConnState::Connecting;

    if (reconnect_times_ == 0) {
        do_connect();
    } else {
        auto ts = kConnectInterval *  reconnect_times_;
        if (ts > kConnectMaxInterval) {
            ts = kConnectMaxInterval;
        }
        deadline_.expires_after(std::chrono::seconds(ts));
        deadline_.async_wait(std::bind(&JimConn::do_connect, shared_from_this()));
    }

    CBLOG_INFO("reconnect ip: %s times: %d", remote_addr_.c_str(), reconnect_times_);
    ++reconnect_times_;
}

void JimConn::do_close() {
    if (state_ == ConnState::Closed) {
        return;
    }

    state_ = ConnState::Closed;
    deadline_.cancel();

    std::error_code ec;
    socket_.close(ec);

    ClearMsg();

    CBLOG_INFO("fd: %d closed. ip: %s", fd_, remote_addr_.c_str());
}

void JimConn::do_idle() {
    reconnect_times_ = 0;
    deadline_.expires_after(std::chrono::seconds(kIdleTime));
    deadline_.async_wait(std::bind(&JimConn::Heartbeat, shared_from_this()));
}

void JimConn::Heartbeat() {
    net::Head head;
    head.msg_type = net::kDataRequestType;
    head.func_id = dspb::kFuncHeartbeat;

    head.Encode();

    auto self(shared_from_this());
    asio::async_write(socket_, asio::buffer(&head, net::kHeadSize),
        [this, self](std::error_code ec, std::size_t length) {
            if (ec) {
                CBLOG_ERROR("heartbeat fd: %d %s, error: %s",
                    fd_, remote_addr_.c_str(), ec.message().c_str());
                do_reconnect();
            }
        });
}

void JimConn::read_head() {
    CBLOG_TRACE("read head begin, fd: %d %s", fd_, remote_addr_.c_str());

    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(&head_, sizeof(head_)),
        [this, self](std::error_code ec, std::size_t) {
            if (!ec) {
                CBLOG_TRACE("read head end, fd: %d %s", fd_, remote_addr_.c_str());
                head_.Decode();
                auto ret = head_.Valid();
                if (ret.ok()) {
                    read_body();
                } else {
                    CBLOG_ERROR("invalid rpc head: fd: %d %s %s",
                        fd_, remote_addr_.c_str(), ret.ToString().c_str());
                    do_reconnect();
                }
            } else {
                if (ec == asio::error::eof) {
                    CBLOG_INFO("read rpc head fd: %d %s, error: %s",
                        fd_, remote_addr_.c_str(), ec.message().c_str());
                } else {
                    CBLOG_ERROR("read rpc head fd: %d %s, error: %s",
                        fd_, remote_addr_.c_str(), ec.message().c_str());
                }
                do_reconnect();
            }
        });
}

void JimConn::read_body() {
    if (head_.body_length == 0) {
        if (head_.func_id == net::kHeartbeatFuncID) { // response heartbeat
            do_idle();
            return read_head();
        }

        return CBLOG_WARN("read body length zero, fd: %d %s ",
            fd_, remote_addr_.c_str());
    }

    CBLOG_TRACE("read body begin: fd: %d %s", fd_, remote_addr_.c_str());

    deadline_.expires_after(std::chrono::seconds(kIdleTime));

    body_.resize(head_.body_length);
    auto self(shared_from_this());
    asio::async_read(socket_, asio::buffer(body_.data(), body_.size()),
        [this, self](std::error_code ec, std::size_t length) {
            CBLOG_TRACE("read body end: fd: %d %s", fd_, remote_addr_.c_str());
            if (!ec) {
                auto it = sent_msgs_.find(head_.msg_id);
                if (it != sent_msgs_.end()) {
                    auto msg = it->second;
                    sent_msgs_.erase(it);

                    //auto end = asio::steady_timer::clock_type::now();
                    //std::chrono::duration<float> diff = end-msg->expire_at() + asio::chrono::milliseconds(100);
                    //CBLOG_DEBUG("read msg id: %" PRIu64 " take times :%fms",msg->msg_id(), diff.count() * 1000);

                    ParseBody(msg);
                } else {
                    CBLOG_WARN("Timeout msg id: %" PRIu64, head_.msg_id);
                }

                read_head();
            } else {
                if (ec == asio::error::eof) {
                    CBLOG_INFO("fd: %d %s read rpc body error: %s",
                        fd_, remote_addr_.c_str(), ec.message().c_str());
                } else {
                    CBLOG_ERROR("fd: %d %s read rpc body error: %s",
                        fd_, remote_addr_.c_str(), ec.message().c_str());
                }
                do_reconnect();
            }
        });
}

void JimConn::do_send() {
    if (write_msgs_.empty()) {
        return;
    }

    auto msg = write_msgs_.front();
    write_msgs_.pop_front();

    do {
        if (msg->cmd()->valid()) {
            break;
        }

        msg->Timeout();

        msg = write_msgs_.front();
        write_msgs_.pop_front();
    } while (true);

    if (msg->timeout()) {
        msg->Timeout();
    } else {
        do_write(msg);
    }
}

void JimConn::do_write(MsgPtr msg) {
    // prepare write buffer
    std::vector<asio::const_buffer> buffers{
        asio::buffer(&msg->msg().head, net::kHeadSize),
        asio::buffer(msg->msg().body.data(), msg->msg().body.size())
    };

    auto self(shared_from_this());
    asio::async_write(socket_, buffers,
        [this, self, msg](std::error_code ec, std::size_t length) {
            //auto end = asio::steady_timer::clock_type::now();
            //std::chrono::duration<float> diff = end-msg->expire_at() + asio::chrono::milliseconds(100);
            //CBLOG_DEBUG("write msg id: %" PRIu64 " take times :%f ms",msg->msg_id(), diff.count() * 1000);

            if (!ec) {
                do_send();
            } else {
                CBLOG_ERROR("fd: %d %s write message error: %s",
                    fd_, remote_addr_.c_str(), ec.message().c_str());
                do_reconnect();
            }
        });
}

bool JimConn::Send(MsgPtr& msg) {
    if (state_ != ConnState::Established) {
        CBLOG_TRACE("Connection is not Established. ip: %s", remote_addr_.c_str());
        return false;
    }
    if (msg->timeout()) {
        CBLOG_WARN("Timeout msg id %" PRIu64, msg->msg_id());
        msg->Complete();
        return true;
    }

    auto self(shared_from_this());
    asio::post(socket_.get_executor(), [this, self, msg] {
        if (msg->timeout()) {
            CBLOG_WARN("Timeout msg id %" PRIu64 " ip: %s",
                msg->msg_id(), remote_addr_.c_str());
            return msg->Complete();
        }

        if (state_ == ConnState::Established) {
            sent_msgs_[msg->msg_id()] = msg;

            auto timer = std::make_shared<asio::steady_timer>(socket_.get_executor());

            timer->expires_at(msg->expire_at());
            timer->async_wait(std::bind(&JimMessage::TimeoutHandle, msg,
                    [this, msg]()-> int {return sent_msgs_.erase(msg->msg_id());}, _1));

            msg->timer_set(timer);

            if (write_msgs_.empty()) {
                do_write(msg);
            } else {
                write_msgs_.push_back(msg);
            }
        } else {
            auto assist = JimClient::Instance().GetAssist();
            assist->AddReSendMsg(msg);
        }
    });

    return true;
}


void JimConn::ClearMsg() {
    auto assist = JimClient::Instance().GetAssist();

    write_msgs_.clear();

    for (auto it=sent_msgs_.begin(); it!=sent_msgs_.end();) {
        auto msg = it->second;
        msg->TimerCancel();
        if (msg->timeout()) {
            msg->Complete();
        } else {
            assist->AddReSendMsg(msg);
        }
        it = sent_msgs_.erase(it);
    }
}

bool JimConn::CheckError(RespPtr resp) {
    if (!resp->header().has_error()) {
        return true;
    }

    auto cluster_id = resp->header().cluster_id();
    auto cluster = JimClient::Instance().GetCluster(cluster_id);
    if (cluster == nullptr) {
        CBLOG_WARN("cluster %" PRIu64 " not found.", cluster_id);
        return false;
    }

    auto error = resp->header().error();
    if (error.has_cluster_mismatch()) {
        CBLOG_WARN("cluster mistatch, node id: %" PRIu64, node_id_);

        cluster->DeleteNode(node_id_);
        return false;
    }

    if (error.has_range_not_found()) {
        auto range_not_found = error.range_not_found();
        CBLOG_WARN("range not found, range id: %" PRIu64, range_not_found.range_id());

        cluster->DeleteRoute(range_not_found.range_id());
        return false;
    }

    if (error.has_out_of_bound()) {
        auto out_of_bound = error.out_of_bound();
        CBLOG_WARN("out of bound, range id: %" PRIu64, out_of_bound.range_id());

        cluster->DeleteRoute(out_of_bound.range_id());
        return false;
    }

    if (error.has_not_leader()) {
        auto leader = error.not_leader();
        CBLOG_WARN("not leader, range: %" PRIu64 " leader: %" PRIu64,
            leader.range_id(), leader.leader().node_id());

        cluster->UpdateRange(leader.range_id(), leader.leader().node_id(), leader.epoch());
        return false;
    }

    if (error.has_stale_epoch()) {
        CBLOG_WARN("range id: %" PRIu64 " stale epoch, other range id: %" PRIu64,
            error.stale_epoch().old_range().id(), error.stale_epoch().new_range().id());

        cluster->UpdateRange(error.stale_epoch().old_range());
        cluster->UpdateRange(error.stale_epoch().new_range());
        return false;
    }

    // other error
    return true;
}

void JimConn::ParseBody(MsgPtr msg) {
    CBLOG_TRACE("parse body: fd: %d %s", fd_, remote_addr_.c_str());

    RespPtr resp = std::make_shared<dspb::RangeResponse>();
    auto len = static_cast<int>(body_.size());
    resp->ParseFromArray(body_.data(), len);

    msg->TimerCancel();
    if (CheckError(resp)) {
        msg->cmd()->add_response(resp);
        msg->Complete();
    } else {
        auto assist = JimClient::Instance().GetAssist();
        assist->AddReSendMsg(msg);
    }
}


} // namespace sdk
} // namespace jimkv

