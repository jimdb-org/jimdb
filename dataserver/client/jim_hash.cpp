
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
//

#include "jim_hash.h"
#include <string.h>

namespace jim {
namespace sdk {

uint16_t murmur_hash(const char *key, size_t len) {
    uint64_t seed = 0x1234ABCD;
    uint64_t m = 0xc6a4a7935bd1e995;
    uint64_t r = 47;
    uint64_t h = seed ^ (len * m);
    uint64_t k;

    const char *tmp = NULL;
    for (tmp = key; len >= 8; ) {
        k = *(uint64_t *)(&tmp[0]);
        tmp += 8;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        len -= 8;
    }

    uint64_t value = 0;
    char remaining[8] = {0,};
    if (len > 0) {
        memcpy(remaining, tmp, len);
        uint64_t *tmp = (uint64_t*)remaining;
        value = *tmp;
        h ^= value;
        h *= m;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return ((uint16_t)h);
}
} //namespace sdk
} //namespace jim
