#include <stdio.h>
#include <sys/time.h>
#include "batch_stress.h"
#include "jimdb_client.h"
#include "jim_log.h"

#define LIST_WRITE_CMD_SIZE  10
#define LIST_READ_CMD_SIZE   3

#define LIST_WAIT_MAX_TIME   60

static bs_command_call_t write_commands[LIST_WRITE_CMD_SIZE];
static bs_command_call_t read_commands[LIST_READ_CMD_SIZE];

void list_command(char *key, unsigned int *seed, int key_code, int mode) {
    bs_command_call_t call;

    int hash_code = rand_r(seed);

    if (mode == BS_WRITE_COMMAND) {
        call = write_commands[hash_code % LIST_WRITE_CMD_SIZE];
    } else {
        call = read_commands[hash_code % LIST_READ_CMD_SIZE];
    }

    return call(key, seed);
}

static void rds_blpop(char *key, unsigned int *seed) {
    cmd_argv_repeat("blpop", key, seed, BS_REPEAT_KEY, 0, rand_r(seed) % LIST_WAIT_MAX_TIME);
}

static void rds_brpop(char *key, unsigned int *seed) {
    cmd_argv_repeat("brpop", key, seed, BS_REPEAT_KEY, 0, rand_r(seed) % LIST_WAIT_MAX_TIME);
}
/*
static void rds_brpoplpush(char *key, unsigned int *seed) {
    char buf[64];

    int desc_len;
    char *desc = gen_rand_string(&desc_len, seed, BS_KEY);

    sprintf(buf, "%d", rand_r(seed) % LIST_WAIT_MAX_TIME);
    cmd_argc_4("brpoplpush", key, desc, strdup(buf));
}
*/
static void rds_lindex(char *key, unsigned int *seed) {
    char buf[64];
    int val = rand_r(seed);
    if (val % 13 % 2) {
        sprintf(buf, "-%d", rand_r(seed));
    } else {
        sprintf(buf, "%d", rand_r(seed));
    }

    cmd_argc_3("lindex", key, strdup(buf));
}

static void rds_linsert(char *key, unsigned int *seed) {
    int cmd_size = 5;
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;
    int code = rand_r(seed);

    static const char *position[] = {"before", "after"};

    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0;
    cmd[i] = strdup("linsert");
    cmd_len[i++] = strlen("linsert");

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    const char *pos = position[code % 2];
    cmd[i] = strdup(pos);
    cmd_len[i++] = strlen(pos);

    int pivot_len = rand_r(seed) % BS_KEY_MAX_LEN;
    cmd[i] = gen_rand_string(&pivot_len, seed, BS_KEY);
    cmd_len[i++] = pivot_len;

    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd[i] = gen_rand_string(&val_len, seed, BS_VAL);
    cmd_len[i++] = val_len;

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char **)cmd, cmd_len, 0);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: linsert, arg_size: %d", cmd_size);
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}

static void rds_llen(char *key, unsigned int *seed) {
    cmd_argc_2("llen", key);
}

static void rds_lpop(char *key, unsigned int *seed) {
    cmd_argc_2("lpop", key);
}

static void rds_lpush(char *key, unsigned int *seed) {
    cmd_argv_repeat("lpush", key, seed, BS_REPEAT_VAL, 1, 0);
}

static void rds_lpushx(char *key, unsigned int *seed) {
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("lpushx", key, gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_lrange(char *key, unsigned int *seed) {
    cmd_range("lrange", key, seed);
}

static void rds_lrem(char *key, unsigned int *seed) {
    char buf[64];
    int code = rand_r(seed);

    if (code % 13 % 2) {
        sprintf(buf, "-%d", code % 100);
    } else {
        sprintf(buf, "%d", code % 100);
    }
    int val_len;

    cmd_argc_4("lrem", key, strdup(buf), gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_lset(char *key, unsigned int *seed) {
    char buf[64];
    int code = rand_r(seed);

    if (code % 13 % 2) {
        sprintf(buf, "-%d", code % 100);
    } else {
        sprintf(buf, "%d", code % 100);
    }
    int val_len;

    cmd_argc_4("lset", key, strdup(buf), gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_ltrim(char *key, unsigned int *seed) {
    cmd_range("ltrim", key, seed);
}

static void rds_rpop(char *key, unsigned int *seed) {
    cmd_argc_2("rpop", key);
}
/*
static void rds_rpoplpsuh(char *key, unsigned int *seed) {
    int desc_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("rpoplpush", key, gen_rand_string(&desc_len, seed, BS_VAL));
}
*/
static void rds_rpush(char *key, unsigned int *seed) {
    cmd_argv_repeat("rpush", key, seed, BS_REPEAT_VAL, 1, 0);
}

static void rds_rpushx(char *key, unsigned int *seed) {
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("rpushx", key, gen_rand_string(&val_len, seed, BS_VAL));
}


void list_init() {
    int i = 0;

    //write_commands[i++] = rds_brpoplpush;
    write_commands[i++] = rds_linsert;
    write_commands[i++] = rds_lpush;
    write_commands[i++] = rds_lpushx;
    write_commands[i++] = rds_lrem;
    write_commands[i++] = rds_lset;
    write_commands[i++] = rds_ltrim;
    //write_commands[i++] = rds_rpoplpsuh;
    write_commands[i++] = rds_rpush;
    write_commands[i++] = rds_rpushx;
    //write_commands[i++] = rds_blpop;
    //write_commands[i++] = rds_brpop;
    write_commands[i++] = rds_lpop;
    write_commands[i++] = rds_rpop;

    i = 0;
    read_commands[i++] = rds_lindex;
    read_commands[i++] = rds_llen;
    read_commands[i++] = rds_lrange;
}
