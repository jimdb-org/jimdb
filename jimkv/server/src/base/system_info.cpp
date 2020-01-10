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

#include "system_info.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string>
#include <map>
#include <iostream>

#ifdef __linux__
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#include <sys/types.h>
#elif defined(__APPLE__)
#include <sys/param.h>
#include <sys/mount.h>
#else
#error unsupport platform
#endif

namespace jimkv {

static bool g_in_docker = false;

void SetSysinfoConfig(bool in_docker) {
    g_in_docker = in_docker;
}

bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) {
    struct statfs buf;
    memset(&buf, 0, sizeof(buf));
    int ret = ::statfs(path, &buf);
    if (ret == 0) {
        *total = buf.f_bsize * buf.f_blocks;
        *available = buf.f_bsize * buf.f_bavail;
        return true;
    } else {
        return false;
    }
}

#ifdef __linux__
static const std::string kMemTotal = "MemTotal";
static const std::string kMemAvailable= "MemAvailable";
static const std::string kMemFree = "MemFree";
static const std::string kMemBuffers = "Buffers";
static const std::string kMemCached = "Cached";

static bool parseMeminfo(std::map<std::string, uint64_t> &info) {
    FILE *fp = ::fopen("/proc/meminfo", "r");
    if (fp == NULL) {
        perror("open /proc/meminfo");
        return false;
    }

    // NOTE: close file when return
    bool ret = true;
    char line[256] = {'\0'};
    while (fgets(line, sizeof(line), fp)) {
        std::string str(line);
        auto pos = str.find(':');
        if (pos == std::string::npos) {
            ret = false;
            break;
        }
        auto name = str.substr(0, pos);
        if (name == kMemTotal || name == kMemAvailable || name == kMemFree ||
            name == kMemBuffers || name == kMemCached) {
            auto val = strtoull(str.substr(pos+1).c_str(), NULL, 10);
            info.emplace(name, val * 1024);
        }
    }
    ::fclose(fp);
    return ret;
}

static bool getLinuxMemoryUsage(uint64_t *total, uint64_t *available) {
    std::map<std::string, uint64_t> info;
    if (!parseMeminfo(info)) {
        return false;
    }

    auto it = info.find(kMemTotal);
    if (it != info.end()) {
        *total = it->second;
    } else {
        return false;
    }

    it = info.find(kMemAvailable);
    if (it != info.end()) {
        *available = it->second;
    } else { // compatible with old kernel
        *available = info[kMemFree] + info[kMemBuffers] + info[kMemCached];
    }
    return true;
}

static const char* kCGroupMemLimitFile = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
static const char* kCGroupMemUsageFile = "/sys/fs/cgroup/memory/memory.usage_in_bytes";

static bool readCGroupFile(const char* file_path, uint64_t *val) {
    // open
    auto fd = ::open(file_path, O_RDONLY);
    if (fd <= 0) {
        std::cerr << "open " << file_path << ": " << strerror(errno) << std::endl;
        return false;
    }

    // read
    char buf[32] = {'\0'};
    auto ret = ::read(fd, buf, sizeof(buf));
    if (ret <= 0) {
        std::cerr << "read " << file_path << ": " << strerror(errno) << std::endl;
        ::close(fd);
        return false;
    }

    // to integer
    *val = strtoull(buf, NULL, 10);
    ::close(fd);
    return true;
}

static bool getDockerMemoryUsage(uint64_t *total, uint64_t *available) {
    auto ret = readCGroupFile(kCGroupMemLimitFile, total);
    if (!ret) {
        return false;
    }

    uint64_t used = 0;
    ret = readCGroupFile(kCGroupMemUsageFile, &used);
    if (!ret) {
        return false;
    }

    *available = (used < *total) ? (*total - used) : 0;
    return true;
}
#endif

bool GetMemoryUsage(uint64_t *total, uint64_t *available) {
#ifdef __linux__
    if (!g_in_docker) {
        return getLinuxMemoryUsage(total, available);
    } else {
        return getDockerMemoryUsage(total, available);
    }
#elif defined(__APPLE__)
    // TODO: fix me
    *total = 8UL * 1024 * 1024 * 1024;
    *available = 4UL * 1024 * 1024 * 1024;
    return true;
#else
#error unsupport platform
#endif
}

} // namespace jimkv
