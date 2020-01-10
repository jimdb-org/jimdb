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

#include "util.h"

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <errno.h>

namespace jimkv {

static thread_local unsigned seed = time(nullptr);

int randomInt() { return rand_r(&seed); }

std::string randomString(size_t length) {
    static const char chars[] = "abcdefghijklmnopqrstuvwxyz"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    std::string str;
    str.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        str.push_back(chars[randomInt() % (sizeof(chars) - 1)]);
    }
    return str;
}

std::string randomString(size_t min_length, size_t max_length) {
    if (min_length >= max_length) {
        return "";
    }
    auto length = min_length + randomInt() % (max_length - min_length);
    return randomString(length);
}

std::string strErrno(int errno_copy) {
    static thread_local char errbuf[1025] = {'\0'};
#ifdef __linux__
    char *ret = ::strerror_r(errno_copy, errbuf, 1024);
    return std::string(ret);
#elif defined(__APPLE__)
    ::strerror_r(errno_copy, errbuf, 1024);
    return std::string(errbuf);
#else
#error unsupport platform
#endif
}

std::string EncodeToOct(const std::string& src) {
    std::string result;
    result.reserve(src.size() * 4);
    char buf[5];
    for (std::string::size_type i = 0; i < src.size(); ++i) {
        if (!isprint(src[i])) {

            snprintf(buf, 5, "\\%03o", static_cast<unsigned char>(src[i]));
            result.append(buf, 4);
        } else {
            result.push_back(src[i]);
        }
    }
    return result;
}

std::string EncodeToHex(const std::string& src) {
    std::string result;
    result.reserve(src.size() * 2);
    char buf[3];
    for (std::string::size_type i = 0; i < src.size(); ++i) {
        snprintf(buf, 3, "%02X", static_cast<unsigned char>(src[i]));
        result.append(buf, 2);
    }
    return result;
}

// most of the code is from rocksdb
static int fromHex(char c) {
    if (c >= 'a' && c <= 'f') {
        c -= ('a' - 'A');
    }
    if (c < '0' || (c > '9' && (c < 'A' || c > 'F'))) {
        return -1;
    }
    if (c <= '9') {
        return c - '0';
    }
    return c - 'A' + 10;
}

bool DecodeFromHex(const std::string& hex, std::string* result) {
    auto len = hex.size();
    if (len % 2 != 0 || result == nullptr) {
        return false;
    }

    result->clear();
    result->reserve(len/2);
    for (size_t i = 0; i < len; i += 2) {
        int h1 = fromHex(hex[i]);
        if (h1 < 0) {
            return false;
        }
        int h2 = fromHex(hex[i+1]);
        if (h2 < 0) {
            return false;
        }
        result->push_back(static_cast<char>((h1 << 4) | h2));
    }
    return true;
}

std::string SliceSeparate(const std::string &l, const std::string &r, size_t max_len) {
    if (l.empty() || r.empty()) {
        return std::string();
    }

    size_t l_len = l.length();
    size_t r_len = r.length();

    size_t len = l_len;

    if (l_len > r_len) {
        len = r_len;
    }

    int cr = 0;
    for (size_t i = 0; i < len; i++) {
        cr = l[i] - r[i];
        if (cr > 0) {
            return l.substr(0, i + 1);
        }
        if (cr < 0) {
            return r.substr(0, i + 1);
        }
        if (max_len > 0 && i > max_len) {
            return r.substr(0, i);
        }
    }

    if (l_len == r_len) {
        return l;
    }
    if (l_len > r_len) {
        return l.substr(0, len + 1);
    }

    return r.substr(0, len + 1);
}

std::string NextComparable(const std::string& str) {
    std::string result;
    if (!str.empty()) {
        result.reserve(str.size());
    }
    for (auto it = str.crbegin(); it != str.crend(); ++it) {
        auto ch = static_cast<uint8_t>(*it);
        if (ch < 0xff) {
            result.assign(str.cbegin(), it.base());
            result.back() = static_cast<char>(ch + 1);
            return result;
        }
    }
    return result;
}

void AnnotateThread(pthread_t handle, const char *name) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    pthread_setname_np(handle, name);
#endif
#endif
}

size_t GetThreadID() {
#ifdef __linux__
    return (size_t)syscall(SYS_gettid);
#elif __APPLE__
    uint64_t thread_id = 0;
    pthread_threadid_np(NULL, &thread_id);
    return static_cast<size_t>(thread_id);
#else
    return 0;
#endif
}

std::string GetThreadName() {
    char buf[32] = {'\0'};
    pthread_getname_np(pthread_self(), buf, 32);
    return buf;
}

int ParseBytesValue(const char* str, int64_t* value) {
    char *end = nullptr;
    errno = 0;
    long v = strtol(str, &end, 10);
    if (errno != 0) {
        return -1;
    }

    if (end == NULL || *end == '\0') {
        *value = v;
    } else if (*end == 'k' || *end == 'K') {
        *value = v * 1024;
    } else if (*end == 'm' || *end == 'M') {
        *value = v * 1024 * 1024;
    }  else if (*end == 'g' || *end == 'G') {
        *value = v * 1024 * 1024 * 1024;
    }  else if (*end == 't' || *end == 'T') {
        *value = v * 1024 * 1024 * 1024 * 1024;
    }  else if (*end == 'p' || *end == 'P') {
        *value = v * 1024 * 1024 * 1024 * 1024 * 1024;
    } else {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static size_t commonPrefixLen(const std::string& left, const std::string& right) {
    size_t common_len = 0;
    auto len = std::min(left.size(), right.size());
    for (size_t i = 0; i < len; ++i) {
        if (left[i] == right[i]) {
            ++common_len;
        } else {
            break;
        }
    }
    return common_len;
}

static uint64_t approximateInt(const std::string& str, size_t offset) {
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) {
        auto ch = offset < str.size() ?
                static_cast<uint8_t>(str[offset]) :
                static_cast<uint8_t>(0);
        val <<= 8;
        val += ch;
        ++offset;
    }
    return val;
}

static std::string approximateStr(uint64_t value) {
    std::string result;
    result.push_back(static_cast<char>(value >> 56));
    result.push_back(static_cast<char>(value >> 48));
    result.push_back(static_cast<char>(value >> 40));
    result.push_back(static_cast<char>(value >> 32));
    result.push_back(static_cast<char>(value >> 24));
    result.push_back(static_cast<char>(value >> 16));
    result.push_back(static_cast<char>(value >> 8));
    result.push_back(static_cast<char>(value));
    return result;
}

std::string FindMiddle(const std::string& left, const std::string& right) {
    if (left >= right) {
        return "";
    }
    auto common_len = commonPrefixLen(left, right);
    auto left_val = static_cast<double>(approximateInt(left, common_len));
    auto right_val = static_cast<double>(approximateInt(right, common_len));
    auto mid_val = static_cast<uint64_t>((left_val + right_val) / 2);

    std::string result;
    if (common_len > 0) {
        result.assign(left.begin(), left.begin() + common_len);
    }
    result += approximateStr(mid_val);
    while (!result.empty() && result.back() == '\0') {
        result.pop_back();
    }

    return result;
}

int64_t NowMicros() {
    timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int64_t NowMilliSeconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + (int64_t)tv.tv_usec / 1000;
}

int64_t NowSeconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec;
}

} /* namespace jimkv */
