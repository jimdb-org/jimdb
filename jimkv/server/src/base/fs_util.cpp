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

#include "fs_util.h"

#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <dirent.h>
#include <libgen.h>

namespace jimkv {

#ifdef __linux__
static const char kPathSeparator = '/';
#elif defined(__APPLE__)
static const char kPathSeparator = '/';
#else
#error unsupported platform
#endif

std::string JoinFilePath(const std::vector<std::string> &strs) {
    std::string ret = strs.empty() ? "" : strs[0];
    for (size_t i = 1; i < strs.size(); ++i) {
        ret.push_back(kPathSeparator);
        ret += strs[i];
    }
    return ret;
}

bool CheckFileExist(const std::string& file) {
    struct stat sb;
    memset(&sb, 0, sizeof(sb));

    if (::stat(file.c_str(), &sb) != 0) {
        return false;
    } else if (!S_ISREG(sb.st_mode)) {
        errno = EACCES;
        return false;
    }
    return true;
}

bool CheckDirExist(const std::string& path) {
    struct stat sb;
    memset(&sb, 0, sizeof(sb));

    if (::stat(path.c_str(), &sb) != 0) {
        return false;
    } else if (!S_ISDIR(sb.st_mode)) {
        errno = ENOTDIR;
        return false;
    }
    return true;
}

bool MakeDirAll(const std::string &path, mode_t mode) {
    struct stat sb;
    memset(&sb, 0, sizeof(sb));

    if (0 == ::stat(path.c_str(), &sb)) {
        if (!S_ISDIR(sb.st_mode)) {  // path exist, but not dir
            errno = ENOTDIR;
            return false;
        } else {
            return true;
        }
    } else if (errno != ENOENT) {
        return false;
    }

    size_t i = path.length();
    while (i > 0 && path[i - 1] == kPathSeparator) {
        --i;
    }

    size_t j = i;
    while (j > 0 && path[j - 1] != kPathSeparator) {
        --j;
    }

    if (j > 1) {
        if (!MakeDirAll(path.substr(0, j), mode)) {
            return false;
        }
    }

    if (0 == ::mkdir(path.c_str(), mode)) {
        return true;
    }
    return errno == EEXIST ? true : false;
}

bool RemoveDirAll(const char *name) {
    struct stat st;
    DIR *dir;
    struct dirent *de;
    int fail = 0;

    if (lstat(name, &st) < 0) {
        return false;
    }

    if (!S_ISDIR(st.st_mode)) {
        return remove(name) == 0;
    }

    dir = opendir(name);
    if (dir == NULL) {
        return false;
    }

    errno = 0;
    while ((de = readdir(dir)) != NULL) {
        char dn[PATH_MAX];
        if (!strcmp(de->d_name, "..") || !strcmp(de->d_name, ".")) {
            continue;
        }
        sprintf(dn, "%s/%s", name, de->d_name);
        if (!RemoveDirAll(dn)) {
            fail = 1;
            break;
        }
        errno = 0;
    }
    if (fail || errno < 0) {
        int save = errno;
        closedir(dir);
        errno = save;
        return false;
    }

    if (closedir(dir) < 0) {
        return false;
    }

    return rmdir(name) == 0;
}

bool ListDirFiles(const std::string& dir_path, std::vector<std::string>& result_files) {
    DIR* dir = ::opendir(dir_path.c_str());
    if (NULL == dir) {
        return false;
    }
    int ret = 0;
    struct dirent* ent = NULL;
    while (true) {
        errno = 0;
        ent = ::readdir(dir);
        if (NULL == ent) {
            if (errno != 0) {
                ret = -1;
            }
            break;
        }

        std::string filename(ent->d_name);
        bool is_regular = false;
        if (ent->d_type == DT_UNKNOWN) {
            struct stat sb;
            memset(&sb, 0, sizeof(sb));
            auto absolute_file = JoinFilePath({dir_path, filename});
            if (::lstat(absolute_file.c_str(), &sb) != 0) {
                ret = -1;
                break;
            }
            is_regular = S_ISREG(sb.st_mode);
        } else if (ent->d_type == DT_REG) {
            is_regular = true;
        }
        if (is_regular) {
            result_files.push_back(std::move(filename));
        }
    }
    closedir(dir);
    return ret == 0;
}

std::string GetDirName(const std::string& path) {
    char tmp[PATH_MAX] = {'\0'};
    strncpy(tmp, path.c_str(), std::min<size_t>(path.size(), PATH_MAX));
    return ::dirname(tmp);
}

std::string GetBaseName(const std::string& path) {
    char tmp[PATH_MAX] = {'\0'};
    strncpy(tmp, path.c_str(), std::min<size_t>(path.size(), PATH_MAX));
    return ::basename(tmp);
}

} /* namespace jimkv */
