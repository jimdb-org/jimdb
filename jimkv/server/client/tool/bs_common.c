#include <time.h>
#include <sys/time.h>
#include "batch_stress.h"
#include "jimkv_client.h"
#include "jimkv_log.h"

static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
static int charset_len = sizeof(charset) - 1;

uint16_t mur_hash(char *key, int len) {
    uint64_t seed = 0x1234ABCD;
    uint64_t m = 0xc6a4a7935bd1e995;
    uint64_t r = 47;
    uint64_t h = seed ^ (len * m);
    uint64_t k;

    char *tmp = NULL;
    for (tmp = key; len >= 8; len -= 8) {
        k = *(uint64_t *)(&tmp[0]);
        tmp += 8;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }

    uint64_t value = 0;
    char remaining[8] = {0};
    if (len > 0) {
        memcpy(remaining, tmp, len);
        value = *(uint64_t *)(&remaining[0]);
        h ^= value;
        h *= m;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return ((uint16_t)h);
}

char *gen_pattern_string(int *length, unsigned int *seed) {
    char buf[16];
    int code = rand_r(seed) % 100;
    int len = code % 16;

    if (len == 0) {
        *length = 1;
        return strdup("*");
    }

    int offset = 0;

    //0: *, 1: ?, 2: [], >=3: charset
    int pattern;
    while (offset < len) {
        pattern = rand_r(seed) % 10;
        switch (pattern) {
            case 0:
                if (offset == 0 || buf[offset - 1] != '*') { // excluding **
                    buf[offset++] = '*';
                }
                break;
            case 1:
                buf[offset++] = '?';
                break;
            case 2:
                if (offset + 3 < len) {
                    buf[offset++] = '[';
                    buf[offset++] = charset[rand_r(seed) % charset_len];
                    buf[offset++] = ']';
                }
                break;
            default:
                buf[offset++] = charset[rand_r(seed) % charset_len];
        }
    }

    *length = offset;
    buf[offset]= '\0';
    return strdup(buf);
}

char *gen_rand_string(int *length, unsigned int *seed, int type) {
    int n;
    char *random_str = NULL;

    if (type == BS_VAL && *length < BS_STR_DEFAULT_LEN) {
        *length += BS_STR_DEFAULT_LEN;
    }

    if (type == BS_KEY && *length < 10) {
        *length += 10;
    }

    if (*length <= 0) {
        *length = BS_STR_DEFAULT_LEN;
    }

    int len = *length;

    random_str = malloc(sizeof(char) * (len + 1));
    if (random_str) {
        for (n = 0; n < len; n++) {
            if (n == 1) {
                random_str[n] = '{';
            } else if (n == 8) {
                random_str[n] = '}';
            } else {
                random_str[n] = charset[rand_r(seed) % charset_len];
            }
        }
        random_str[len] = '\0';
    }

    return random_str;
}

void cmd_argc_1(const char *command) {
    char *cmd[1];
    size_t cmd_len[1];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;

    int i = 0;
    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, 1, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, 1, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s", command);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<1; i++) {
        free(cmd[i]);
    }
}


void cmd_argc_2(const char *command, char *key) {
    char *cmd[2];
    size_t cmd_len[2];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;
    int i = 0;
    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    cmd[i] = key;
    cmd_len[i++] = strlen(key);
    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, 2, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, 2, cmd);
        jimkv_reply_free(rep);
    }
    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    /*
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s %s", command, key);
    }
    */
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<2; i++) {
        free(cmd[i]);
    }
}

void cmd_argc_3(const char *command, char *key, char *val) {
    char *cmd[3];
    size_t cmd_len[3];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;

    int i = 0;
    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    cmd[i] = val;
    cmd_len[i++] = strlen(val);

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, 3, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, 3, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s %s %s", command, key, val);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<3; i++) {
        free(cmd[i]);
    }
}

void cmd_argc_4(const char *command, char *key, char *argv1, char *argv2) {
    char *cmd[4];
    size_t cmd_len[4];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;

    int i = 0;
    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    cmd[i] = argv1;
    cmd_len[i++] = strlen(argv1);

    cmd[i] = argv2;
    cmd_len[i++] = strlen(argv2);

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, 4, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, 4, cmd);
        jimkv_reply_free(rep);
    }
    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s %s %s %s", command, key, argv1, argv2);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<4; i++) {
        free(cmd[i]);
    }
}

void cmd_argv_repeat(const char *command, char *key, unsigned int *seed,
        int repeat_flat, int supply_val, int timeout)
{
    char buf[64];
    int key_len, val_len;
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;
    
    int cmd_size = 2;
    int code = rand_r(seed);

    int repeat_num = code % BS_MULTI_MAX_SIZE;

    cmd_size += supply_val;

    if (repeat_flat & BS_REPEAT_KEY) {
        cmd_size += repeat_num;
    }

    if (repeat_flat & BS_REPEAT_VAL) {
        cmd_size += repeat_num;
    }

    if (timeout > 0) {
        cmd_size += 1;
    }

    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0, j = 0;

    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    while (supply_val) {
        if (supply_val-- % 2 != 0 && (repeat_flat & BS_REPEAT_VAL)) {
            val_len = rand_r(seed) % BS_VAL_MAX_LEN;
            cmd[i] = gen_rand_string(&val_len, seed, BS_VAL);
        } else {
            val_len = rand_r(seed) % BS_KEY_MAX_LEN;
            cmd[i] = gen_rand_string(&val_len, seed, BS_KEY);
        }
        cmd_len[i++] = val_len;
    }

    for (j=0; j<repeat_num; j++) {
        if (repeat_flat & BS_REPEAT_KEY) {
            key_len = rand_r(seed) % BS_KEY_MAX_LEN;
            cmd[i] = gen_rand_string(&key_len, seed, BS_KEY);
            cmd_len[i++] = key_len;
        }

        if (repeat_flat & BS_REPEAT_VAL) {
            val_len = rand_r(seed) % BS_VAL_MAX_LEN;
            cmd[i] = gen_rand_string(&val_len, seed, BS_VAL);
            cmd_len[i++] = val_len;
        }
    }

    if (timeout > 0) {
        cmd_len[i] = sprintf(buf, "%d", timeout);
        cmd[i++] = strdup(buf);
    }

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char**)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s, arg_size: %d", command, cmd_size);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}

void cmd_scan(const char *command, char *key, unsigned int *seed) {
    char buf[32];
    int cmd_size = 3;
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;

    int code = rand_r(seed);
    int match = code % 19 % 2;
    int count = code % 23 % 2;

    if (match == 1) {
        cmd_size += 2;
    }
    if (count == 1) {
        cmd_size += 2;
    }
    /*
    if (key != NULL) {
        cmd_size += 1;
    }
    */
    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0;
    cmd[i] = strdup(command);
    cmd_len[i++] = strlen(command);

    if (key != NULL) {
        cmd[i] = key;
        cmd_len[i++] = strlen(key);
    } else {//slot id
        cmd_len[i] = sprintf(buf, "%d", code % 100);
        cmd[i++] = strdup(buf);
    }

    // cursor
    cmd_len[i] = sprintf(buf, "%d", code % 100);
    cmd[i++] = strdup(buf);

    if (match == 1) {
        cmd[i] = strdup("match");
        cmd_len[i++] = strlen("match");

        int pattern_len = 0;
        char *pattern = gen_pattern_string(&pattern_len, seed);
        cmd[i] = pattern;
        cmd_len[i++] = pattern_len;
    }

    if (count == 1) {
        cmd[i] = strdup("count");
        cmd_len[i++] = strlen("count");

        cmd_len[i] = sprintf(buf, "%d", code % 100);
        cmd[i++] = strdup(buf);
    }

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char**)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: %s", command);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}

void cmd_range(const char *command, char *key, unsigned int *seed) {
    char start[64];
    char end[64];

    int start_rand = rand_r(seed);
    int end_rand = rand_r(seed);

    if (start_rand % 13 % 2) {
        sprintf(start, "-%d", start_rand % BS_VAL_MAX_LEN);
    } else {
        sprintf(start, "%d", start_rand % BS_VAL_MAX_LEN);
    }

    if (end_rand % 13 % 2) {
        sprintf(end, "-%d", end_rand % BS_VAL_MAX_LEN);
    } else {
        sprintf(end, "%d", end_rand % BS_VAL_MAX_LEN);
    }

    cmd_argc_4(command, key, strdup(start), strdup(end));
}
void print_cmd(const int argc, char **argv, const int out_buf_len, char *out_buf)
{
    int i;
    int all_buf_len = 0;
    int buf_len = 0;
    for (i = 0; i< argc; i++) {
        if (all_buf_len > out_buf_len) {
            return;
        }
        buf_len = sprintf(out_buf+all_buf_len, "%s ", argv[i]);
        all_buf_len += buf_len;
    }
}

void format_reply(jimkv_reply_t **reps, int reply_count, const int out_buf_len, char *out_buf)
{
    int i;
    int all_buf_len = 0;
    int buf_len = 0;
    if (reply_count <= 0) {
        sprintf(out_buf, "empty list or set");
        return;
    }
    for (i = 0; i < reply_count; i++) {
        if (all_buf_len > out_buf_len) {
            return;
        }
        if (reps[i]) {
            switch (reps[i]->type) {
                case JIMDB_REPLY_STRING:
                case JIMDB_REPLY_STATUS:
                    buf_len = sprintf(out_buf+all_buf_len, "%s ", reps[i]->str);
                    break;
                case JIMDB_REPLY_ERROR:
                    buf_len = sprintf(out_buf+all_buf_len, "%s ", reps[i]->str);
                    break;
                case JIMDB_REPLY_INTEGER:
                    buf_len = sprintf(out_buf+all_buf_len, "%lld ", reps[i]->integer);
                    break;
                case JIMDB_REPLY_NIL:
                    buf_len = sprintf(out_buf+all_buf_len, "[nil] ");
                    break;
                case JIMDB_REPLY_ARRAY:
                    buf_len = sprintf(out_buf+all_buf_len, "in_array");
                    break;
                default:
                    CBLOG_ERROR("fatal protocol error");
            }
            all_buf_len += buf_len;
        }else {
            CBLOG_ERROR("reply is null");
        }
    }
}
void print_reply(void *response, int argc, char** argv) {
    char buf[1048576];
    char resp_buf[1048576];
    jimkv_reply_t *rep = response;
    if (rep) {
        switch (rep->type) {
            case JIMDB_REPLY_STRING:
            case JIMDB_REPLY_STATUS:
                print_cmd(argc, argv, 1048576, buf);
                CBLOG_DEBUG("cmd: %s, result: %s", buf, rep->str);
                break;
            case JIMDB_REPLY_ERROR:
                print_cmd(argc, argv, 1048576, buf);
                CBLOG_DEBUG("cmd: %s, result: %s", buf, rep->str);
                break;
            case JIMDB_REPLY_INTEGER:
                print_cmd(argc, argv, 1048576, buf);
                CBLOG_DEBUG("cmd: %s, result: %lld", buf, rep->integer);
                break;
            case JIMDB_REPLY_NIL:
                print_cmd(argc, argv, 1048576, buf);
                CBLOG_DEBUG("cmd: %s, result: key not found", buf);
                break;
            case JIMDB_REPLY_ARRAY:
                print_cmd(argc, argv, 1048576, buf);
                format_reply(rep->element, rep->elements, 1048576, resp_buf);
                CBLOG_DEBUG("cmd: %s, result: %s", buf, resp_buf);
                break;
            default:
                CBLOG_INFO("protocol error");
        }
    } else {
        CBLOG_ERROR("can not get reply");
    }
}



