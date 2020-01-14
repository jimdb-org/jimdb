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

#include "transport.h"
#include "base/shared_mutex.h"
#include "net/server.h"
#include "net/context_pool.h"
#include "net/session.h"
#include "raft/options.h"

namespace jim {
namespace raft {
namespace impl {
namespace transport {

class TcpConnection : public Connection {
public:
    explicit TcpConnection(const std::shared_ptr<net::Session>& session);

    Status Send(MessagePtr& msg) override;
    Status Close() override;

private:
    std::weak_ptr<net::Session> session_;
};

using TcpConnPtr = std::shared_ptr<TcpConnection>;


class TcpTransport : public Transport {
public:
    explicit TcpTransport(const TransportOptions& ops);

    ~TcpTransport();

    Status Start(const std::string& listen_ip, uint16_t listen_port,
            const MessageHandler& handler) override;

    void Shutdown() override;

    void SendMessage(MessagePtr& msg) override;

    Status GetConnection(uint64_t to, std::shared_ptr<Connection>* conn) override;

private:
    using CreateConnFunc = std::function<Status(uint64_t, TcpConnPtr&)>;

    class ConnectionGroup {
    public:
        explicit ConnectionGroup(const CreateConnFunc& create_func);

        ConnectionGroup(const ConnectionGroup&) = delete;
        ConnectionGroup& operator=(const ConnectionGroup&) = delete;

        TcpConnPtr Get(uint64_t to);
        void Remove(uint64_t to, const TcpConnPtr& conn);

    private:
        CreateConnFunc create_func_;
        std::unordered_map<uint64_t, TcpConnPtr> connections_;
        mutable jim::shared_mutex mu_;
    };

private:
    void onMessage(const net::Context& ctx, const net::MessagePtr& msg);
    Status newConnection(uint64_t to, TcpConnPtr& conn);

private:
    std::shared_ptr<NodeResolver> resolver_;

    std::unique_ptr<net::Server> server_;
    MessageHandler handler_;

    std::vector<std::unique_ptr<ConnectionGroup>> conn_pool_;
    std::hash<uint64_t> hash_func_;
    std::unique_ptr<net::IOContextPool> client_;
    net::SessionOptions client_opt_;
};


} /* namespace transport */
} /* namespace impl */
} /* namespace raft */
} /* namespace jim */
