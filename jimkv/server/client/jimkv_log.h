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
//

#ifndef __JIMKV_LOG_H__
#define __JIMKV_LOG_H__

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

#define JIMKV_LINE_MAX    2048

#define JIMKV_LOG_ERROR   0
#define JIMKV_LOG_WARN    1
#define JIMKV_LOG_INFO    2
#define JIMKV_LOG_TRACE   3
#define JIMKV_LOG_DEBUG   4

#ifdef __GNUC__
#define JIMKV_FMT(a, b) __attribute__((format(printf, a, b)))
#else
#define JIMKV_FMT(a, b)
#endif

typedef void (*jimkv_log_callback_t) (int level, const char *logstr, void *user_data);

void jimkv_log_level_set(int level);
void jimkv_log_callback_set(jimkv_log_callback_t callback, void *user_data);

void jimkv_log_error(const char *format, ...) JIMKV_FMT(1, 2);
void jimkv_log_warn(const char *format, ...)  JIMKV_FMT(1, 2);
void jimkv_log_info(const char *format, ...)  JIMKV_FMT(1, 2);
void jimkv_log_trace(const char *format, ...) JIMKV_FMT(1, 2);
void jimkv_log_debug(const char *format, ...) JIMKV_FMT(1, 2);

#define CBLOG_TRACE(fmt, ...) \
    jimkv_log_trace("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__)

#define CBLOG_DEBUG(fmt, ...) \
    jimkv_log_debug("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__)

#define CBLOG_INFO(fmt, ...)  \
    jimkv_log_info("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__)

#define CBLOG_WARN(fmt, ...)  \
    jimkv_log_warn("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__)

#define CBLOG_ERROR(fmt, ...) \
    jimkv_log_error("%s:%d " fmt, __FNAME__, __LINE__, ##__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif // __JIMKV_LOG_H__
