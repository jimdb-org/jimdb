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

#include "base/status.h"
#include "dspb/txn.pb.h"
#include "table.h"

namespace jim {
namespace test {
namespace helper {

class SelectResultParser {
public:
    SelectResultParser(const dspb::SelectRequest& req,
                       const dspb::SelectResponse& resp);

    const std::vector<std::vector<std::string>>& GetRows() const {
        return rows_;
    }

    const std::vector<std::string>& GetKeys() const {
        return keys_;
    }

    Status Match(const std::vector<std::vector<std::string>>& expected_rows) const;

private:
    std::vector<std::vector<std::string>> rows_;
    std::vector<std::string> keys_;
};

class SelectFlowResultParser {
public:
    SelectFlowResultParser(const dspb::SelectFlowRequest& req,
                       const dspb::SelectFlowResponse& resp);

    const std::vector<std::vector<std::string>>& GetRows() const {
        return rows_;
    }

    const std::vector<std::string>& GetKeys() const {
        return keys_;
    }

    Status Match(const std::vector<std::vector<std::string>>& expected_rows) const;

private:
    std::vector<std::vector<std::string>> rows_;
    std::vector<std::string> keys_;
};

} /* namespace helper */
} /* namespace test */
} /* namespace jim */
