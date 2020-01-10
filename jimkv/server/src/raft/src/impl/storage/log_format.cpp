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

#include "log_format.h"

#include <assert.h>
#include <string.h>
#include <iomanip>
#include <sstream>
#include <regex>

#include "base/byte_order.h"

namespace jimkv {
namespace raft {
namespace impl {
namespace storage {

std::string makeLogFileName(uint64_t seq, uint64_t offset) {
    std::stringstream s;
    s << std::hex << std::setfill('0') << std::setw(16) << seq;
    s << "-";
    s << std::hex << std::setfill('0') << std::setw(16) << offset;
    s << ".log";
    return s.str();
}

bool parseLogFileName(const std::string &name, uint64_t &seq,
                      uint64_t &offset) {
    std::string front;
    std::string end;
    std::size_t pos = name.find('-');
    std::size_t plog = name.find(".log");
    if (pos != std::string::npos && plog != std::string::npos) {
        std::string suffix = name.substr(plog);
        if (suffix != ".log") return false;

        front = name.substr(0, 16);
        end = name.substr(pos + 1, 16);

        if (front.size() < 16 || end.size() < 16) {
            return false;
        } else {
            for (int num = 0; num < 16; num++) {
                char c1 = front.at(num);
                char c2 = end.at(num);
                if (!((c1 >= '0' && c1 <= '9') || (c1 >= 'a' && c1 <= 'f'))) {
                    return false;
                }

                if (!((c2 >= '0' && c2 <= '9') || (c2 >= 'a' && c2 <= 'f'))) {
                    return false;
                }
            }
        }
    } else {
        return false;
    }

    seq = std::stoull(front, 0, 16);
    offset = std::stoull(end, 0, 16);
    return true;
}

void Footer::Encode() {
    version = htobe16(version);
    index_offset = htobe32(index_offset);
}

void Footer::Decode() {
    version = be16toh(version);
    index_offset = be32toh(index_offset);
}

static std::string formatMagic(const char magic[4]) {
    std::string result;
    char buf[3];
    for (int i = 0; i < 4; ++i) {
        snprintf(buf, 3, "%02x", static_cast<unsigned char>(magic[i]));
        result.append(buf, 2);
    }
    return result;
}

Status Footer::Validate() const {
    if (strncmp(magic, kLogFileMagic, strlen(kLogFileMagic)) != 0) {
        return Status(Status::kCorruption, "invalid log footer",
                      std::string("magic: ") + formatMagic(magic));
    }
    if (index_offset == 0) {
        return Status(Status::kCorruption, "invalid log footer index offset",
                      std::to_string(index_offset));
    }
    return Status::OK();
}

void Record::Encode() {
    size = htobe32(size);
    crc = htobe32(crc);
}

void Record::Decode() {
    size = be32toh(size);
    crc = be32toh(size);
}

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace jimkv */
