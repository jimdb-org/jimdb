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

_Pragma ("once");

#include <string>

namespace jim {
namespace sdk {

class JimScope {
public:
    JimScope() = default;
    ~JimScope() = default;

    JimScope(const std::string &start, const std::string &end) :
        start_(start), end_(end)
    {
    }

    JimScope(const std::string &key) :
        start_(key), end_(key)
    {
        end_[end_.size() - 1] += 1;
    }

    JimScope(const JimScope &other) {
        start_ = other.start_;
        end_ = other.end_;
    }

    JimScope(JimScope &&other) {
        start_.swap(other.start_);
        end_.swap(other.end_);
    }

    JimScope& operator=(const JimScope &other) {
        if (this != &other) {
            start_ = other.start_;
            end_ = other.end_;
        }
        return *this;
    }

    JimScope& operator=(JimScope &&other) {
        if (this != &other) {
            start_.swap(other.start_);
            end_.swap(other.end_);
        }
        return *this;
    }

    bool operator<(const JimScope& other) const {
        return end_ <= other.start_ && start_ != other.start_;
    }

    std::string start() const {return start_;}

    std::string end() const {return end_;}

private:
    std::string start_;
    std::string end_;
};

} // namespace sdk
} // namespace jim

