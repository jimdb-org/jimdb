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

#ifdef __linux__
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htobe16(x) OSSwapInt16(x)
#define htole16(x) (x)
#define be16toh(x) OSSwapInt16(x)
#define le16toh(x) (x)

#define htobe32(x) OSSwapInt32(x)
#define htole32(x) (x)
#define be32toh(x) OSSwapInt32(x)
#define le32toh(x) (x)

#define htobe64(x) OSSwapInt64(x)
#define htole64(x) (x)
#define be64toh(x) OSSwapInt64(x)
#define le64toh(x) (x)

#elif __BYTE_ORDER == __BIG_ENDIAN
#define htobe16(x) (x)
#define htole16(x) OSSwapInt16(x)
#define be16toh(x) (x)
#define le16toh(x) OSSwapInt16(x)

#define htobe32(x) (x)
#define htole32(x) OSSwapInt32(x)
#define be32toh(x) (x)
#define le32toh(x) OSSwapInt32(x)

#define htobe64(x) (x)
#define htole64(x) OSSwapInt64(x)
#define be64toh(x) (x)
#define le64toh(x) OSSwapInt64(x)

#else
#error unknown machine byte order
#endif

#else
#error unsupported platform
#endif
