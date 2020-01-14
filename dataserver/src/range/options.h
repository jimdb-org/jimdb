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


namespace jim {
namespace ds {
namespace range {

// RangeOptions range parameters
struct RangeOptions {
    // range heartbeat interval milliseconds
    size_t heartbeat_interval_msec = 10;

    // for split
    bool enable_split = true;
    size_t check_size = 32UL * 1024 * 1024;
    size_t split_size = 64UL * 1024 * 1024;
    size_t max_size = 128L * 1024 * 1024;

    // if a request's process time beyond this threshold, a warnning log is print
    int64_t request_warn_usecs = 500000;
};

} // namespace range
} // namespace ds
} // namespace jim
