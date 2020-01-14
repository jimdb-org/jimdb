// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
// Modified work copyright 2019 The JIMDB Authors.
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

#include "status.h"
#include <cstring>

namespace jim {

const char *Status::copyState(const char *s) {
    char *const result = new char[strlen(s) + 1];
    strcpy(result, s);
    return result;
}

Status::Status(Code code, const std::string &msg1, const std::string &msg2)
    : code_(code) {
    const size_t len1 = msg1.size();
    const size_t len2 = msg2.size();
    const size_t size = len1 + (len2 > 0 ? (len2 + 2) : 0);
    char *const result = new char[size + 1];
    memcpy(result, msg1.data(), len1);
    if (len2 > 0) {
        result[len1] = ':';
        result[len1 + 1] = ' ';
        memcpy(result + len1 + 2, msg2.data(), len2);
    }
    result[size] = '\0';
    state_ = result;
}

std::string Status::ToString() const {
    char tmp[30];
    const char *type;
    switch (code_) {
        case kOk:
            return "ok";
        case kNotFound:
            type = "NotFound: ";
            break;
        case kCorruption:
            type = "Corruption: ";
            break;
        case kNotSupported:
            type = "NotSupported: ";
            break;
        case kInvalidArgument:
            type = "Invalid argument: ";
            break;
        case kIOError:
            type = "IO error: ";
            break;
        case kShutdownInProgress:
            type = "Shutdown in progress: ";
            break;
        case kTimedOut:
            type = "Timedout: ";
            break;
        case kAborted:
            type = "Aborted: ";
            break;
        case kBusy:
            type = "Busy: ";
            break;
        case kExpired:
            type = "Expired: ";
            break;
        case kDuplicate:
            type = "Duplicate: ";
            break;
        case kCompacted:
            type = "Compacted: ";
            break;
        case kEndofFile:
            type = "EOF: ";
            break;
        case kNoLeader:
            type = "No Leader: ";
            break;
        case kNotLeader:
            type = "Not Leader: ";
            break;
        case kStaleEpoch:
            type = "StaleEpoch: ";
            break;
        case kExisted:
            type = "Existed: ";
            break;
        case kNoMem:
            type = "No Memory: ";
            break;
        case kStaleRange:
            type = "Stale Range: ";
            break;
        case kInvalid:
            type = "Invalid: ";
            break;
        case kResourceExhaust:
            type = "Resource is Exhaust: ";
            break;
        case kNoLeftSpace:
            type = "No Left Space: ";
            break;
        case kUnexpected:
            type = "Unexpected: ";
            break;
        case kOutOfBound:
            type = "Out Of Bound: ";
            break;
        case kNotChange:
            type = "Data Not Chanage: ";
            break;
        case kNoMoreData:
            type = "No More Data: ";
            break;
        case kTypeConflict:
            type = "Type Conflict: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code_));
            type = tmp;
    }
    std::string result(type);
    if (state_ != nullptr) {
        result.append(state_);
    }
    return result;
}

} /* namespace jim */
