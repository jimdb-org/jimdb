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

#include <array>
#include "base/status.h"

namespace jim {
namespace net {

static const uint32_t kMagic = 0x23232323;
static const uint16_t kCurrentVersion = 1;

static const uint16_t kAdminRequestType = 0x01;
static const uint16_t kAdminResponseType = 0x11;
static const uint16_t kDataRequestType = 0x02;
static const uint16_t kDataResponseType = 0x12;

static const uint32_t kMaxBodyLength = 20 * 1024 * 1024;  // 20Mb

static const uint16_t kHeartbeatFuncID = 0;

enum class Flag : uint8_t {
    kForceFastWorker = 1 << 0,
};

struct Head {
    uint32_t magic = kMagic;
    uint16_t version = kCurrentVersion;
    uint16_t msg_type = 0;
    uint16_t func_id = 0;
    uint64_t msg_id = 0;
    uint8_t flags = 0;
    uint8_t proto_type = 0;
    uint32_t timeout = 0;
    uint32_t body_length = 0;

    // set from a request head, self is a response head
    void SetResp(const Head& req, uint32_t body_len);

    // encode to network byte order
    void Encode();
    // decode network byte order to host order
    void Decode();

    Status Valid() const;
    std::string DebugString() const;

    bool ForceFastFlag() const;

} __attribute__((packed));

static constexpr int kHeadSize = sizeof(Head);

}  // namespace net
}  // namespace jim
