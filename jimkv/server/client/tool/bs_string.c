#include <sys/time.h>
#include "batch_stress.h"
#include "jimkv_client.h"
#include "jimkv_log.h"
#define STR_DIGIT_CMD_SIZE  0

#define STR_WRITE_CMD_SIZE  2
#define STR_READ_CMD_SIZE   2

#define STR_DIGIT_TYPE      0

static bs_command_call_t digit_commands[STR_DIGIT_CMD_SIZE];

static bs_command_call_t write_commands[STR_WRITE_CMD_SIZE];
static bs_command_call_t read_commands[STR_READ_CMD_SIZE];

void string_command(char *key, unsigned int *seed, int key_code, int mode) {
    bs_command_call_t call;

    int hash_code = rand_r(seed);

    if (mode == BS_WRITE_COMMAND) {
        if (key_code % 10 == STR_DIGIT_TYPE) {
            //call = digit_commands[hash_code % STR_DIGIT_CMD_SIZE];
            call = write_commands[hash_code % STR_WRITE_CMD_SIZE];
        } else {
            call = write_commands[hash_code % STR_WRITE_CMD_SIZE];
        }
    } else {
        call = read_commands[hash_code % STR_READ_CMD_SIZE];
    }

    return call(key, seed);
}

static void rds_append(char *key, unsigned int *seed) {
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("append", key, gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_bitcount(char *key, unsigned int *seed) {
    char buf[64];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;
    int cmd_size = 2;
    int code = rand_r(seed);

    //0 no; 1 start; 2 start end
    int range = code % 3;
    cmd_size  += range;

    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0, j = 0;

    cmd[i] = strdup("bitcount");
    cmd_len[i++] = strlen("bitcount");

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    for (j=0; j<range; j++) {
        if (code % 13 % 2) {
            cmd_len[i] = sprintf(buf, "-%d", rand_r(seed) % BS_VAL_MAX_LEN);
        } else {
            cmd_len[i] = sprintf(buf, "%d", rand_r(seed) % BS_VAL_MAX_LEN);
        }
        cmd[i++] = strdup(buf);
    }

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }

    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: bitcount");
    }
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}
static void rds_bitop(char *key, unsigned int *seed) {
    int cmd_size = 3;
    int code = rand_r(seed);

    static const char *ops[] = {"and", "or", "xor", "not"};

    //0 no; 1 start; 2 start end
    int op_len = code % BS_MULTI_MAX_SIZE;
    cmd_size  += op_len;

    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0, j = 0;

    cmd[i] = strdup("bitop");
    cmd_len[i++] = strlen("bitop");

    const char *op = ops[code % 4];
    cmd[i] = strdup(op);
    cmd_len[i++] = strlen(op);

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    for (j=0; j<op_len; j++) {
        int val_len = rand_r(seed) % BS_KEY_MAX_LEN;
        cmd[i] = gen_rand_string(&val_len, seed, BS_KEY);
        cmd_len[i++] = val_len;
    }

    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char **)cmd, cmd_len, 100);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }

    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}

// >= 3.2.0
//static void rds_bitfield(char *key, unsigned int *seed) {
//
//}

static void rds_decr(char *key, unsigned int *seed) {
    cmd_argc_2("decr", key);
}

static void rds_decrby(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "-%d", rand_r(seed));
    cmd_argc_3("decrby", key, strdup(buf));
}

static void rds_get(char *key, unsigned int *seed) {
    cmd_argc_2("get", key);
}

static void rds_getbit(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%d", rand_r(seed) % BS_VAL_MAX_LEN);
    cmd_argc_3("getbit", key, strdup(buf));
}

static void rds_getrange(char *key, unsigned int *seed) {
    cmd_range("getrange", key, seed);
}

static void rds_getset(char *key, unsigned int *seed) {
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("getset", key, gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_incr(char *key, unsigned int *seed) {
    cmd_argc_2("incr", key);
}

static void rds_incrby(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%d", rand_r(seed));
    cmd_argc_3("incrby", key, strdup(buf));
}

static void rds_incrbyfloat(char *key, unsigned int *seed) {
    char buf[64];
    int val = rand_r(seed);
    float divisor = (val % 13 + 1) * 1.0;

    if (val % 13 % 2) {
        sprintf(buf, "-%f", rand_r(seed) / divisor);
    } else {
        sprintf(buf, "%f", rand_r(seed) / divisor);
    }

    cmd_argc_3("incrbyfloat", key, strdup(buf));
}
static void rds_mget(char *key, unsigned int *seed) {
    cmd_argv_repeat("mget", key, seed, BS_REPEAT_KEY, 0, 0);
}

static void rds_mset(char *key, unsigned int *seed) {
    cmd_argv_repeat("mset", key, seed, BS_REPEAT_KEY|BS_REPEAT_VAL, 1, 0);
}

static void rds_msetnx(char *key, unsigned int *seed) {
    cmd_argv_repeat("msetnx", key, seed, BS_REPEAT_KEY|BS_REPEAT_VAL, 1, 0);
}
static void rds_psetex(char *key, unsigned int *seed) {
    char ms[64];
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;

    sprintf(ms, "%d", rand_r(seed) % BS_DAY_MILLISECOND);
    cmd_argc_4("psetex", key, strdup(ms), gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_set(char *key, unsigned int *seed) {
    char buf[64];
    struct timeval tv_begin,tv_end;
    uint64_t timeinterval;

    int cmd_size = 3;
    int code = rand_r(seed);

    //0 no; 1 ex seconds; 2 px milliseconds
    //int expire = code % 19 % 3;
    int expire = 0;
    if (expire != 0) {
        cmd_size += 2;
    }

    //0 no; 1 NX; 2 XX
    int x = code % 23 % 3;
    if (x != 0) {
        cmd_size += 1;
    }

    int val_len = code % BS_VAL_MAX_LEN;

    char **cmd = malloc(sizeof(char*) * cmd_size);
    size_t *cmd_len = malloc(sizeof(size_t) * cmd_size);

    int i = 0;
    cmd[i] = strdup("set");
    cmd_len[i++] = 3;

    cmd[i] = key;
    cmd_len[i++] = strlen(key);

    cmd[i] = gen_rand_string(&val_len, seed, BS_VAL);
    cmd_len[i++] = val_len;

    if (expire == 1) {
        cmd[i] = strdup("EX");
        cmd_len[i++] = 2;

        int s = rand_r(seed) % BS_DAY_SECOND;
        cmd_len[i] = sprintf(buf, "%d", s);
        cmd[i++] = strdup(buf);
    } else if (expire == 2) {
        cmd[i] = strdup("PX");
        cmd_len[i++] = 2;

        int s = rand_r(seed) % BS_DAY_MILLISECOND;
        cmd_len[i] = sprintf(buf, "%d", s);
        cmd[i++] = strdup(buf);
    }

    if (x == 1) {
        cmd[i] = strdup("NX");
        cmd_len[i++] = 2;
    } else if (x == 2) {
        cmd[i] = strdup("XX");
        cmd_len[i++] = 2;
    }

    gettimeofday(&tv_begin,NULL);
    jimkv_reply_t *rep = jimkv_command_exec(space_context, cmd_size, (const char **)cmd, cmd_len, 100);
    gettimeofday(&tv_end,NULL);
    if (rep != NULL) {
        print_reply(rep, cmd_size, cmd);
        jimkv_reply_free(rep);
    }
    timeinterval =(tv_end.tv_sec - tv_begin.tv_sec)*1000000 + (tv_end.tv_usec - tv_begin.tv_usec);
    /*
    if (timeinterval>10000) {
        CBLOG_WARN("exceed 10ms for cmd: set, cmd_size: %d", cmd_size);
    }
    */
    __sync_fetch_and_add(&g_req_number, 1);
    __sync_fetch_and_add(&g_req_time, timeinterval);
    for (i=0; i<cmd_size; i++) {
        free(cmd[i]);
    }
    free(cmd);
    free(cmd_len);
}

static void rds_setbit(char *key, unsigned int *seed) {
    char offset[64];
    char val[64];

    sprintf(offset, "%d", rand_r(seed) % BS_VAL_MAX_LEN);
    sprintf(val, "%d", rand_r(seed) % 2);

    cmd_argc_4("setbit", key, strdup(offset), strdup(val));
}

static void rds_setex(char *key, unsigned int *seed) {
    char seconds[64];
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;

    sprintf(seconds, "%d", rand_r(seed) % BS_DAY_SECOND);
    cmd_argc_4("setex", key, strdup(seconds), gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_setnx(char *key, unsigned int *seed) {
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;
    cmd_argc_3("setnx", key, gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_setrange(char *key, unsigned int *seed) {
    char offset[64];
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;

    sprintf(offset, "%d", rand_r(seed) % BS_VAL_MAX_LEN);
    cmd_argc_4("setrange", key, strdup(offset), gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_strlen(char *key, unsigned int *seed) {
    cmd_argc_2("strlen", key);
}

void string_init() {
    int i = 0;

    //write_commands[i++] = rds_append;
    //write_commands[i++] = rds_getset;
    write_commands[i++] = rds_mset;
    //write_commands[i++] = rds_msetnx;
    //write_commands[i++] = rds_psetex;
    write_commands[i++] = rds_set;
    //write_commands[i++] = rds_setex;
    //write_commands[i++] = rds_setnx;
    //write_commands[i++] = rds_setrange;

    i = 0;
    //read_commands[i++] = rds_bitcount;
    read_commands[i++] = rds_get;
    //read_commands[i++] = rds_getbit;
    //read_commands[i++] = rds_getrange;
    read_commands[i++] = rds_mget;
    //read_commands[i++] = rds_strlen;

    i = 0;
    //digit_commands[i++] = rds_bitop;
    //digit_commands[i++] = rds_decr;
    //digit_commands[i++] = rds_decrby;
    //digit_commands[i++] = rds_incr;
    //digit_commands[i++] = rds_incrby;
    //digit_commands[i++] = rds_incrbyfloat;
    //digit_commands[i++] = rds_setbit;
}
