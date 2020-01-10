// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
// Modified work copyright 2019 The JimDB Authors.
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

#include <string>
#include <utility>

namespace jimkv {

class Status {
public:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kShutdownInProgress = 6,
        kTimedOut = 7,
        kAborted = 8,
        kBusy = 9,
        kExpired = 10,
        kDuplicate = 11,
        kCompacted = 12,
        kEndofFile = 13,

        kNoLeader = 14,
        kNotLeader = 15,
        kStaleEpoch = 16,
        kExisted = 17,
        kNoMem = 18,
        kStaleRange = 19,
        kInvalid = 20,
        kResourceExhaust = 21,
        kNoLeftSpace = 22,
        kUnexpected = 23,
        kOutOfBound = 24,

        kNotChange = 25,
        kNoMoreData = 26,
        kTypeConflict = 27,

        kUnknown = 255,
    };

    Status() : code_(kOk), state_(nullptr) {}
    explicit Status(Code code) : code_(code), state_(nullptr) {}
    Status(Code code, const std::string& msg1, const std::string& msg2);
    ~Status() { delete[] state_; }

    Status(const Status& s);
    Status& operator=(const Status& s);

    Status(Status&& s) noexcept;
    Status& operator=(Status&& s) noexcept;

    bool operator==(const Status& s) const;
    bool operator!=(const Status& s) const;

    Code code() const { return code_; }

    static Status OK() { return Status(); }

    bool ok() const { return code_ == kOk; }

    std::string ToString() const;

private:
    static const char* copyState(const char* s);

private:
    Code code_;
    const char* state_;
};

inline Status::Status(const Status& s) : code_(s.code_) {
    state_ = (s.state_ == nullptr) ? nullptr : copyState(s.state_);
}

inline Status& Status::operator=(const Status& s) {
    if (this != &s) {
        code_ = s.code_;
        delete[] state_;
        state_ = (s.state_ == nullptr) ? nullptr : copyState(s.state_);
    }
    return *this;
}

inline Status::Status(Status&& s) noexcept : Status() { *this = std::move(s); }

inline Status& Status::operator=(Status&& s) noexcept {
    if (this != &s) {
        code_ = std::move(s.code_);
        s.code_ = kOk;
        delete[] state_;
        state_ = nullptr;
        std::swap(state_, s.state_);
    }
    return *this;
}

inline bool Status::operator==(const Status& s) const { return code_ == s.code_; }
inline bool Status::operator!=(const Status& s) const { return !(*this == s); }

} /* namespace jimkv */
