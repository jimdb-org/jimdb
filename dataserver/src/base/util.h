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

#include <sys/types.h>
#include <cstdint>
#include <string>

namespace jim {

int randomInt();

std::string randomString(size_t length);
std::string randomString(size_t min_length, size_t max_length);

std::string strErrno(int errno_copy);

std::string EncodeToOct(const std::string& src);

std::string EncodeToHex(const std::string& src);
bool DecodeFromHex(const std::string& hex, std::string* result);

std::string SliceSeparate(const std::string &l, const std::string &r, size_t max_len = 0);

// return a empty string if could not find one
std::string NextComparable(const std::string& str);

void AnnotateThread(pthread_t handle, const char *name);
size_t GetThreadID();
std::string GetThreadName();

int ParseBytesValue(const char* str, int64_t* value);

// left should less than right
std::string FindMiddle(const std::string& left, const std::string& right);

int64_t NowMicros();
int64_t NowMilliSeconds();
int64_t NowSeconds();

} /* namespace jim */
