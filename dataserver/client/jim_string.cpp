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
//
#include "jim_string.h"
#include "jim_log.h"

namespace jim {
namespace sdk {

void print_args(const std::vector<std::string>& args) {
    std::string buf;
    for (auto &arg : args) {
        buf += arg;
        buf += ' ';
    }
    CBLOG_DEBUG("command is %.*s", static_cast<int>(buf.size()), buf.c_str());
}

} //namespace sdk
} //namespace jim
