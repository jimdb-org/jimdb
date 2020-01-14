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

#include <string>
#include <vector>
#include <functional>

#include "base/util.h"

namespace jim {

std::string JoinFilePath(const std::vector<std::string>& strs);

bool CheckFileExist(const std::string& file);

bool CheckDirExist(const std::string& path);

bool MakeDirAll(const std::string &path, mode_t mode);

bool RemoveDirAll(const char *name);

bool ListDirFiles(const std::string& dir_path, std::vector<std::string>& files);

std::string GetDirName(const std::string& path);
std::string GetBaseName(const std::string& path);

} /* namespace jim */
