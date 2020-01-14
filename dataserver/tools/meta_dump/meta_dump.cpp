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

#include <getopt.h>
#include <iostream>

#include "storage/meta_store.h"

using namespace jim::ds::storage;

struct DumpOptions {
    uint64_t range_id = 0;
    bool dump_all_ranges = false;
};

void print_usage(char *name);
void dump(const std::string&, const DumpOptions&);

int main(int argc, char *argv[]) {
    struct option longopts[] = {
            { "path",    required_argument,  NULL,   'p' },
            { "range",   optional_argument,  NULL,   'r' },
            { "all",     no_argument,        NULL,   'a' },
            { "help",    no_argument,        NULL,   'h' },
            { NULL,      0,                  NULL,    0  }
    };

    int ch = 0;
    std::string path;
    DumpOptions ops;
    while ((ch = getopt_long(argc, argv, "p:r:", longopts, NULL)) != -1) {
        switch (ch) {
            case 'p':
                path = optarg;
                break;
            case 'r':
                ops.range_id = strtoul(optarg, NULL, 10);
                break;
            case 'a':
                ops.dump_all_ranges = true;
                break;
            default:
                print_usage(argv[0]);
                return 0;
        }
    }

    dump(path, ops);

    return 0;
}

void dumpRange(MetaStore& store, uint64_t range_id) {
    std::cout << "Range[" << range_id << "]: " << std::endl;

    // dump range's apply index
    uint64_t applied = 0;
    auto s = store.LoadApplyIndex(range_id, &applied);
    if (!s.ok()) {
        std::cerr << "Load Apply index failed: " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    std::cout << "\tApply: \t" << applied << std::endl;

    // dump range's meta
    basepb::Range range;
    s = store.GetRange(range_id, &range);
    if (!s.ok()) {
        std::cerr << "Load Range Meta failed: " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    std::cout << "\tMeta: \t" << range.ShortDebugString() << std::endl;
    std::cout << std::endl;
}

void dumpAllRanges(MetaStore& store) {
    std::vector<basepb::Range> ranges;
    auto s = store.GetAllRange(&ranges);
    if (!s.ok()) {
        std::cerr << "GetAllRange failed: " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    for (const auto& meta: ranges) {
        std::cout << "Range[" << meta.id() << "]: " << std::endl;
        // dump range's apply index
        uint64_t applied = 0;
        s = store.LoadApplyIndex(meta.id(), &applied);
        if (!s.ok()) {
            std::cerr << "Load Range " << meta.id() << " Apply index failed: " << s.ToString() << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cout << "\tApply: \t" << applied << std::endl;
        std::cout << "\tMeta: \t" << meta.ShortDebugString() << std::endl;
        std::cout << std::endl;
    }
}

void dump(const std::string& path, const DumpOptions& ops) {
    MetaStore store(path);
    auto s = store.Open(true);
    if (!s.ok()) {
        std::cerr << "Open Meta failed(path: " << path << "): " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }

    // dump node id
    uint64_t node_id = 0;
    s = store.GetNodeID(&node_id);
    if (!s.ok()) {
        std::cerr << "Get NodeID failed: " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    std::cout << "Path: \t"  << path << std::endl;
    std::cout << "NodeID: \t" << node_id << std::endl;
    std::cout << std::endl;

    if (ops.dump_all_ranges) {
        dumpAllRanges(store);
        return;
    }

    if (ops.range_id != 0) {
        dumpRange(store, ops.range_id);
    }
}

void print_usage(char *name) {
    std::cout << std::endl << name << " - jimdb dataserver meta dump tool" << std::endl << std::endl;

    std::cout << "Usage: " << std::endl;
    std::cout << "\t--path=<meta path>: required, meta directory path" << std::endl;
    std::cout << "\t--range=<range id>: optional, dump one range's metas" << std::endl;
    std::cout << "\t--all: optional, dump all range's metas" << std::endl;

    std::cout << std::endl;
}
