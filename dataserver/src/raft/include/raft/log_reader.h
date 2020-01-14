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
namespace raft {

// LogReader is used for external systems to fetch raft log entries
class LogReader {
public:
    virtual ~LogReader() = default;

    // only return normal log entry, entries such as ConfChange is filtered
    // index: raft index
    // data: user data
    // if status is not ok or over is true, no data will be returned
    virtual Status Next(uint64_t& index, std::string& data, bool& over) = 0;

    // last index has read (include index be filtered)
    virtual uint64_t LastIndex() = 0;
};

} /* namespace raft */
} /* namespace jim */
