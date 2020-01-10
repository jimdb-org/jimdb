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

#include "command_line.h"

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <sstream>

namespace jimkv {
namespace raft {
namespace playground {

uint64_t gNodeID = 1;
std::vector<uint64_t> gPeers{1};

void parseCommandLine(int argc, char *argv[]) {
    int opt = -1;
    std::string peers;
    while ((opt = getopt(argc, argv, "n:p:")) != -1) {
        switch (opt) {
            case 'n':
                gNodeID = atoi(optarg);
                break;
            case 'p':
                peers = optarg;
                break;
            default:
                fprintf(stderr, "Usage: %s [-n node_id] [-p peer_ids] \n",
                        argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    // parse peer ids
    std::istringstream ss(peers);
    std::string p;
    while (std::getline(ss, p, ',')) {
        uint64_t node_id = atoi(p.c_str());
        gPeers.push_back(node_id);
    }

    std::cout << "node id:    " << gNodeID << std::endl;
    std::cout << "peers:      ";
    for (auto p : gPeers) {
        std::cout << p << " ";
    }
    std::cout << std::endl;
}

void checkCommandLineValid() {
    if (gNodeID == 0) {
        fprintf(stderr, "ERR: invalid node id 0\n");
        exit(EXIT_FAILURE);
    }
    if (gPeers.empty()) {
        fprintf(stderr, "ERR: invalid peer ids\n");
        exit(EXIT_FAILURE);
    }
    bool found = false;
    for (auto p : gPeers) {
        if (p == gNodeID) found = true;
        if (p == 0) {
            fprintf(stderr, "ERR: invald peers ids\n");
            exit(EXIT_FAILURE);
        }
    }
    if (!found) {
        fprintf(stderr, "ERR: could not find node id(%lu) in peer ids\n",
                gNodeID);
        exit(EXIT_FAILURE);
    }
}

} /* namespace playground */
} /* namespace raft */
} /* namespace jimkv */