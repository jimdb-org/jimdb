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

_Pragma ("once");

#include <string>

namespace jimkv {
namespace sdk {

class JimScope {
public:
    JimScope() = delete;
    ~JimScope() = default;

    JimScope(const std::string &start, const std::string &end) :
        start_(start), end_(end)
    {
    }

    bool operator<(const JimScope& other) const {
        return end_ <= other.start() && start_ != other.start();
    }

    bool operator<(const std::string& key) const {
        return end_ <= key;
    }

    bool operator>(const std::string& key) const {
        return start_ > key;
    }

    std::string start() const {return start_;}

    std::string end() const {return end_;}

private:
    std::string start_;
    std::string end_;
};

} // namespace sdk
} // namespace jimkv

