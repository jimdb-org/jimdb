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

#include "raft/src/impl/storage/storage_disk.h"
#include "proto/gen/dspb/raft_internal.pb.h"

using namespace jim::raft::impl;
using namespace jim::raft::impl::storage;

struct DumpOptions {
    uint64_t start = 0;
    uint64_t count = 0;
};

void print_usage(char *name);
void dump(const std::string&, const DumpOptions&);

int main(int argc, char *argv[]) {
    struct option longopts[] = {
            { "path",    required_argument,  NULL,   'p' },
            { "start",   optional_argument,  NULL,   's' },
            { "count",   optional_argument,  NULL,   'c' },
            { "help",    no_argument,        NULL,   'h' },
            { NULL,      0,                  NULL,    0  }
    };

    int ch = 0;
    std::string path;
    DumpOptions ops;
    while ((ch = getopt_long(argc, argv, "p:s:c:", longopts, NULL)) != -1) {
        switch (ch) {
            case 'p':
                path = optarg;
                break;
            case 's':
                ops.start = strtoul(optarg, NULL, 10);
                break;
            case 'c':
                ops.count = strtoul(optarg, NULL, 10);
                break;
            default:
                print_usage(argv[0]);
                return 0;
        }
    }

    dump(path, ops);

    return 0;
}

void dumpLogs(DiskStorage &ds, const DumpOptions& ops, uint64_t frist_index, uint64_t last_index) {
    uint64_t lo = std::max(frist_index, ops.start);
    uint64_t count = std::max(ops.count, static_cast<uint64_t>(1));
    uint64_t hi = std::min(last_index + 1, lo + count);
    std::vector<EntryPtr> ents;
    bool compact = false;
    auto s = ds.Entries(lo, hi, std::numeric_limits<std::uint64_t>::max(), &ents, &compact);
    if (!s.ok()) {
        std::cerr << "Load entries[" << lo << "-" << hi << "] failed: "  << s.ToString() << std::endl;
        return;
    }

    for (const auto& e: ents) {
        std::cout << "Entry " << e->index() << " :" << std::endl;
        std::cout << "\tTerm: " << e->term() << std::endl;
        switch (e->type()) {
            case pb::ENTRY_CONF_CHANGE: {
                pb::ConfChange cc;
                cc.ParseFromString(e->data());
                std::cout << "\tConfChange: " << cc.ShortDebugString() << std::endl;
                break;
            }
            case pb::ENTRY_NORMAL: {
                if (e->data().empty()) {
                    std::cout << "\tNil" << std::endl;
                    break;
                }
                dspb::Command cmd;
                cmd.ParseFromString(e->data());
                std::cout << "\tCommand: " << cmd.ShortDebugString() << std::endl;
                break;
            }
            default:
                std::cout << "\t<Invalid Entry Type: " << static_cast<int>(e->type()) << ">"<< std::endl;
        }
        std::cout << std::endl;
    }
}

void dump(const std::string& path, const DumpOptions& ops) {
    // open for readonly
    DiskStorage::Options dops;
    dops.readonly = true;
    DiskStorage ds(1, path, dops);
    auto s = ds.Open();
    if (!s.ok()) {
        std::cerr << "Open log failed(" << path << ") failed: " << s.ToString() << std::endl;
        ::exit(EXIT_FAILURE);
    }

    // dump hard state
    pb::HardState hs;
    s = ds.InitialState(&hs);
    if (!s.ok()) {
        std::cerr << "Load hard state failed: " << s.ToString() << std::endl;
        ::exit(EXIT_FAILURE);
    }
    std::cout << "Term: \t" << hs.term() << std::endl;
    std::cout << "Commit: \t" << hs.commit() << std::endl;
    std::cout << "Vote: \t" << hs.vote() << std::endl;
    std::cout << std::endl;

    // dump log summary
    std::cout << "Log Files: \t" << ds.FilesCount() << std::endl;

    // dump first index
    uint64_t first_index = 0;
    s = ds.FirstIndex(&first_index);
    if (!s.ok()) {
        std::cerr << "Load first index failed: " << s.ToString() << std::endl;
        ::exit(EXIT_FAILURE);
    }
    std::cout << "First Index: \t" << first_index << std::endl;

    // dump last index
    uint64_t last_index = 0;
    s = ds.LastIndex(&last_index);
    if (!s.ok()) {
        std::cerr << "Load last index failed: " << s.ToString() << std::endl;
        ::exit(EXIT_FAILURE);
    }
    std::cout << "Last Index: \t" << last_index << std::endl;

    // dump trunc meta
    if (first_index > 1) {
        uint64_t term = 0;
        bool compacted = false;
        s = ds.Term(first_index-1, &term, &compacted);
        if (!s.ok()) {
            std::cerr << "Load entry(" << first_index-1 << "'s term failed: " << s.ToString() << std::endl;
            ::exit(EXIT_FAILURE);
        }
        std::cout << "Trunc Term: \t" << term << ", " << std::boolalpha << compacted << std::endl;
    }
    std::cout << std::endl;

    if (ops.count != 0 || ops.start != 0) {
        dumpLogs(ds, ops, first_index, last_index);
    }

    ds.Close();
}

void print_usage(char *name) {
    std::cout << std::endl << name << " - jimdb raft log dump tool" << std::endl << std::endl;

    std::cout << "Usage: " << std::endl;
    std::cout << "\t--path=<log path>: required, raft log directory path" << std::endl;
    std::cout << "\t--start=<start log index>: optional, dump log entry start from, default: <FirstIndex>" << std::endl;
    std::cout << "\t--count=<entries number>: optional, dump how many log entries (from <start>), default: 1" << std::endl;

    std::cout << std::endl;
}
