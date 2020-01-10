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

_Pragma("once");

#include <stdint.h>
#include <string>
#include "base/status.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

static const uint16_t kLogCurrentVersion = 1;
static const char* kLogFileMagic = "\x99\xA3\xB8\xDE";

std::string makeLogFileName(uint64_t seq, uint64_t index);
bool parseLogFileName(const std::string& name, uint64_t& seq, uint64_t& index);

struct Footer {
    char magic[4] = {'\0'};
    uint16_t version = kLogCurrentVersion;
    uint32_t index_offset = 0;
    char reserved[54] = {'\0'};

    // convert to big-endian when write to file
    void Encode();
    // conver to host-endian when read from file
    void Decode();

    Status Validate() const;

} __attribute__((packed));

enum RecordType : uint8_t { kLogEntry = 1, kIndex };

struct Record {
    RecordType type = kLogEntry;
    uint32_t size = 0;
    uint32_t crc = 0;
    char payload[0];

    // convert to big-endian when write to file
    void Encode();
    // conver to host-endian when read from file
    void Decode();

} __attribute__((packed));

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
