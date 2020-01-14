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
#include "string_util.h"
namespace jim {

void SplitString(const std::string& str, char c,
                 std::vector<std::string>& res)
{
  res.clear();
  size_t last = 0;
  size_t length = str.size();
  for (size_t i = 0; i <= length; ++i) {
    if (i == length || str[i] == c) {
        if (i - last > 0) {
            res.emplace_back(str, last, i-last);
        }
      last = i + 1;
    }
  }
}

}  // namespace jim
