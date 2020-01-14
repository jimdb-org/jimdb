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

#include <thread>
#include "asio/io_context.hpp"
#include "asio/ip/tcp.hpp"

#include "base/status.h"

#include "message.h"
#include "options.h"

namespace jim {
namespace net {

class IOContextPool;

class Server final {
public:
    Server(const ServerOptions& opt, const std::string& name);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    Status ListenAndServe(const std::string& listen_ip, uint16_t listen_port,
                          const Handler &handler);

    void Stop();

private:
    void doAccept();
    asio::io_context& getContext();

private:
    const std::string name_;
    ServerOptions opt_;
    Handler handler_;

    bool stopped_ = false;

    // acceptor
    asio::io_context context_;
    asio::ip::tcp::acceptor acceptor_;

    std::unique_ptr<IOContextPool> context_pool_;

    std::unique_ptr<std::thread> thr_;
};

}  // namespace net
}  // namespace jim
