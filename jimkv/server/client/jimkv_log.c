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

#include "jimkv_log.h"

#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

static volatile int g_log_level = 0;

static void *log_user_data = NULL;
static jimkv_log_callback_t log_callback = NULL;

int get_tid() {
#ifdef __linux__
    return syscall(SYS_gettid);
#elif __APPLE__
    uint64_t thread_id = 0;
    pthread_threadid_np(NULL, &thread_id);
    return thread_id;
#else
    return 0;
#endif
}

void jimkv_log_level_set(int level) {
    g_log_level = level;
}

void jimkv_log_callback_set(jimkv_log_callback_t callback, void *user_data) {
    log_user_data = user_data;
    log_callback = callback;
}

#define jimkv_do_log(priority, caption)                    \
do {                                                        \
    if (g_log_level < priority)  {                          \
        break;                                              \
    }                                                       \
    char text[JIMKV_LINE_MAX];                             \
    char time_text[64];                                     \
    int text_len;                                           \
                                                            \
    va_list ap;                                             \
    va_start(ap, format);                                   \
    text_len = vsnprintf(text, JIMKV_LINE_MAX, format, ap);\
    va_end(ap);                                             \
                                                            \
    if (log_callback != NULL) {                             \
        return log_callback(priority, text, log_user_data); \
    }                                                       \
                                                            \
    if (text_len >= JIMKV_LINE_MAX) {                      \
        text_len = JIMKV_LINE_MAX - 1;                     \
    }                                                       \
                                                            \
    struct tm tm;                                           \
    struct timeval tv;                                      \
    gettimeofday(&tv, NULL);                                \
    localtime_r(&tv.tv_sec, &tm);                           \
    strftime(time_text, 64, "%F %T",                        \
            localtime_r(&tv.tv_sec, &tm));                  \
                                                            \
    fprintf(stderr, "%s.%04ld %d %s %.*s\n",                \
            time_text, tv.tv_usec, get_tid(),               \
            caption, text_len, text);                       \
} while (0)

void jimkv_log_error(const char *format, ...) {
    jimkv_do_log(JIMKV_LOG_ERROR, "ERROR");
}

void jimkv_log_warn(const char *format, ...) {
    jimkv_do_log(JIMKV_LOG_WARN, "WARN");
}

void jimkv_log_info(const char *format, ...) {
    jimkv_do_log(JIMKV_LOG_INFO, "INFO");
}

void jimkv_log_trace(const char *format, ...) {
    jimkv_do_log(JIMKV_LOG_TRACE, "TRACE");
}

void jimkv_log_debug(const char *format, ...) {
    jimkv_do_log(JIMKV_LOG_DEBUG, "DEBUG");
}
