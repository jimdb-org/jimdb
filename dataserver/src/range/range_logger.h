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

#include <common/logger.h>
#include <stdarg.h>

#define RLOG_DEBUG(fmt, ...)  FLOG_DEBUG("range[{}] " fmt, id_, ##__VA_ARGS__)
#define RLOG_INFO(fmt, ...)  FLOG_INFO("range[{}] " fmt, id_, ##__VA_ARGS__)
#define RLOG_WARN(fmt, ...)  FLOG_WARN("range[{}] " fmt, id_, ##__VA_ARGS__)
#define RLOG_ERROR(fmt, ...)  FLOG_ERROR("range[{}] " fmt, id_, ##__VA_ARGS__)
#define RLOG_CRIT(fmt, ...)  FLOG_CRIT("range[{}] " fmt, id_, ##__VA_ARGS__)
