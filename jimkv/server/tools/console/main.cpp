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

#include <getopt.h>
#include <cstddef>

#include "linenoise/linenoise.h"
#include "spdlog/spdlog.h"

#include "client.h"

using namespace jimkv::ds::tool;
using namespace dspb;

// global client
std::unique_ptr<AdminClient> client;

using DispatchFunc = std::function<void(std::vector<std::string>&)>;
using DispatchMap = std::map<std::string, DispatchFunc>;

bool checkStatus(const jimkv::Status& s) {
    if (!s.ok()) {
        std::cerr << "ERR: " << s.ToString() << std::endl;
        return false;
    } else {
        return true;
    }
}

void parseCfgKey(const std::string& arg, dspb::ConfigKey* key) {
    auto pos = arg.find('.');
    if (pos == std::string::npos) {
        key->set_name(arg);
    } else {
        key->set_section(arg.substr(0, pos));
        key->set_name(arg.substr(pos + 1));
    }
}

void setConfig(std::vector<std::string>& args) {
    if (args.size() != 2) {
        std::cerr << "invalid config set argument size: required 2" << std::endl;
        return;
    }
    dspb::AdminRequest req;
    auto cfg = req.mutable_set_cfg()->add_configs();
    parseCfgKey(args[0], cfg->mutable_key());
    cfg->set_value(args[1]);
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cerr << "OK" << std::endl;
    }
}

void getConfig(std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "invalid config get argument size: required 1" << std::endl;
        return;
    }
    dspb::AdminRequest req;
    parseCfgKey(args[0], req.mutable_get_cfg()->add_key());
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cerr << resp.get_cfg().DebugString() << std::endl;
    }
}

void configCommand(std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "invalid config argument" << std::endl;
        return;
    }
    auto arg0 = args[0];
    args.erase(args.begin());
    if (arg0 == "set") {
        setConfig(args);
    } else if (arg0 == "get") {
        getConfig(args);
    } else {
        std::cerr << "invalid config argument: " << arg0 << std::endl;
    }
}

void infoCommand(std::vector<std::string>& args) {
    dspb::AdminRequest req;
    auto get_info = req.mutable_get_info();
    if (!args.empty()) {
        get_info->set_path(args[0]);
    }
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cout << resp.get_info().data() << std::endl;
    }
}

void compactionCommand(std::vector<std::string>& args) {
    dspb::AdminRequest req;
    auto compaction = req.mutable_compaction();
    if (!args.empty()) {
        compaction->set_range_id(strtoull(args[0].c_str(), nullptr, 10));
    }
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cout << "OK" << std::endl;
    }
}

void profileCommand(std::vector<std::string>& args) {
    if (args.empty()) {
        std::cerr << "invalid profile argument size: required at least 1" << std::endl;
        return;
    }

    dspb::AdminRequest req;
    auto profile = req.mutable_profile();
    auto ptype = args[0];
    args.erase(args.begin());
    if (ptype == "cpu") {
        profile->set_ptype(dspb::ProfileRequest_ProfileType_CPU);
    } else if (ptype == "heap") {
        profile->set_ptype(dspb::ProfileRequest_ProfileType_HEAP);
    } else if (ptype == "rocksdb") {
        profile->set_ptype(dspb::ProfileRequest_ProfileType_ROCKSDB);
        if (args.empty() || (args[0] != "start" && args[0] != "stop")) {
            std::cerr << "invalid proile rocksdb argument: required start or stop" << std::endl;
            return;
        }
        profile->set_op(args[0] == "start" ?
                        dspb::ProfileRequest_ProfileOp_PROFILE_START :
                        dspb::ProfileRequest_ProfileOp_PROFILE_STOP);
    } else {
        std::cerr << "ERR: unsupported profile type: " << args[0] << std::endl;
        return;
    }
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cout << "OK" << std::endl;
    }
}

void dumpCommand(std::vector<std::string>& args) {
    dspb::AdminRequest req;
    req.mutable_dump();
    dspb::AdminResponse resp;
    auto s = client->Admin(req, resp);
    if (checkStatus(s)) {
        std::cout << "OK" << std::endl;
    }
}

static const DispatchMap dispatch_funcs = {
        {"config", configCommand},
        {"info", infoCommand},
        {"compaction", compactionCommand},
        {"profile", profileCommand},
        {"dump", dumpCommand},
};

void dispatch(std::vector<std::string>& args) {
    assert(!args.empty());
    auto cmd = args[0];
    args.erase(args.begin());
    auto it = dispatch_funcs.find(cmd);
    if (it != dispatch_funcs.end()) {
        (it->second)(args);
    } else {
        std::cerr << "ERR: unsupported command: " << cmd << std::endl;
    }
}

std::vector<std::string> splitArgs(const char *line) {
    std::stringstream ss(line);
    std::string arg;
    std::vector<std::string> args_list;
    while (std::getline(ss, arg, ' ')) {
        if (arg == " " || arg.empty()) {
            continue;
        }
        // to lower
        std::transform(arg.begin(), arg.end(), arg.begin(),
                       [](unsigned char c){ return std::tolower(c); });
        args_list.push_back(std::move(arg));
    }
    return args_list;
}


bool isQuit(const std::string& cmd) {
    return cmd == "q" || cmd == "quit" || cmd == "exit";
}

void printUsage(char *name) {
    std::cout << std::endl << name << " - jimkv dataserver console" << std::endl << std::endl;

    std::cout << "Usage: " << std::endl;
    std::cout << "\t--host=<server ip>, or -h: required, target server's host, ip etc" << std::endl;
    std::cout << "\t--port=<admin port>, or -p: required, target server's admin port" << std::endl;

    std::cout << std::endl;
}

int main(int argc, char *argv[]) {
    struct option longopts[] = {
            { "host",    required_argument,  NULL,   'h' },
            { "port",    required_argument,  NULL,   'p' },
            { "help",    no_argument,        NULL,   'k' },
            { NULL,      0,                  NULL,    0  }
    };

    spdlog::set_level(spdlog::level::err);

    int ch = 0;
    std::string ip, port;
    while ((ch = getopt_long(argc, argv, "h:p:", longopts, NULL)) != -1) {
        switch (ch) {
            case 'h':
                ip = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            default:
                printUsage(argv[0]);
                return 0;
        }
    }
    if (ip.empty() || port.empty()) {
        printUsage(argv[0]);
        return 0;
    }

    client.reset(new AdminClient(ip, port));

    class LineReleaser {
    public:
        explicit LineReleaser(char *line) : line_(line) {}
        ~LineReleaser() { linenoiseFree(line_); }

        LineReleaser(const LineReleaser&) = delete;
        LineReleaser& operator=(const LineReleaser&) = delete;
    private:
        char *line_ = nullptr;
    };

    const char *history_file = ".ds-consle.history";
    linenoiseHistoryLoad(history_file);
    const std::string prompt = ip + ":" + port + "> ";
    while (true) {
        char *line = linenoise(prompt.c_str());
        if (line == nullptr) {
            break;
        }
        LineReleaser releaser(line);

        auto args = splitArgs(line);
        if (args.empty()) {
            continue;
        }
        if (isQuit(args[0])) {
            break;
        }

        linenoiseHistoryAdd(line);
        linenoiseHistorySave(history_file);

        dispatch(args);
    }
    return 0;
}
