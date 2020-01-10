#include "batch_stress.h"

#define HASH_WRITE_CMD_SIZE  6
#define HASH_READ_CMD_SIZE   7

static bs_command_call_t write_commands[HASH_WRITE_CMD_SIZE];
static bs_command_call_t read_commands[HASH_READ_CMD_SIZE];

void hash_command(char *key, unsigned int *seed, int key_code, int mode) {
    bs_command_call_t call;

    int hash_code = rand_r(seed);

    if (mode == BS_WRITE_COMMAND) {
        call = write_commands[hash_code % HASH_WRITE_CMD_SIZE];
    } else {
        call = read_commands[hash_code % HASH_READ_CMD_SIZE];
    }

    return call(key, seed);
}

static void rds_hdel(char *key, unsigned int *seed) {
    cmd_argv_repeat("hdel", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_hexists(char *key, unsigned int *seed) {
    int field_len = rand_r(seed) % BS_KEY_MAX_LEN;
    cmd_argc_3("hexists", key, gen_rand_string(&field_len, seed, BS_KEY));
}

static void rds_hget(char *key, unsigned int *seed) {
    int field_len = rand_r(seed) % BS_KEY_MAX_LEN;
    cmd_argc_3("hget", key, gen_rand_string(&field_len, seed, BS_KEY));
}

static void rds_hgetall(char *key, unsigned int *seed) {
    cmd_argc_2("hgetall", key);
}

static void rds_hincrby(char *key, unsigned int *seed) {
    char buf[64];

    int code = rand_r(seed);
    int field_len = code % BS_KEY_MAX_LEN;

    if (code % 13 % 2) {
        sprintf(buf, "-%d", rand_r(seed));
    } else {
        sprintf(buf, "%d", rand_r(seed));
    }

    cmd_argc_4("hincrby", key, gen_rand_string(&field_len, seed, BS_KEY), strdup(buf));
}

static void rds_hincrbyfloat(char *key, unsigned int *seed) {
    char buf[64];

    int code = rand_r(seed);
    int field_len = code % BS_KEY_MAX_LEN;

    float divisor = (code % 13 + 1) * 1.0;

    if (code % 13 % 2) {
        sprintf(buf, "-%f", rand_r(seed) / divisor);
    } else {
        sprintf(buf, "%f", rand_r(seed) / divisor);
    }

    cmd_argc_4("hincrbyfloat", key, gen_rand_string(&field_len, seed, BS_KEY), strdup(buf));
}

static void rds_hkeys(char *key, unsigned int *seed) {
    cmd_argc_2("hkeys", key);
}

static void rds_hlen(char *key, unsigned int *seed) {
    cmd_argc_2("hlen", key);
}

static void rds_hmget(char *key, unsigned int *seed) {
    cmd_argv_repeat("hmget", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_hmset(char *key, unsigned int *seed) {
    cmd_argv_repeat("hmset", key, seed, BS_REPEAT_KEY|BS_REPEAT_VAL, 2, 0);
}

static void rds_hset(char *key, unsigned int *seed) {
    int field_len = rand_r(seed) % BS_KEY_MAX_LEN;
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;

    cmd_argc_4("hset", key, gen_rand_string(&field_len, seed, BS_KEY),
            gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_hsetnx(char *key, unsigned int *seed) {
    int field_len = rand_r(seed) % BS_KEY_MAX_LEN;
    int val_len = rand_r(seed) % BS_VAL_MAX_LEN;

    cmd_argc_4("hsetnx", key, gen_rand_string(&field_len, seed, BS_KEY),
            gen_rand_string(&val_len, seed, BS_VAL));
}

static void rds_hvals(char *key, unsigned int *seed) {
    cmd_argc_2("hvals", key);
}

static void rds_hscan(char *key, unsigned int *seed) {
    cmd_scan("hscan", key, seed);
}

// >= 3.2.0
//static void rds_rds_hstrlen(char *key, unsigned int *seed) {
//}

void batch_hash_init() {
    int i = 0;

    write_commands[i++] = rds_hdel;
    write_commands[i++] = rds_hincrby;
    write_commands[i++] = rds_hincrbyfloat;
    write_commands[i++] = rds_hmset;
    write_commands[i++] = rds_hset;
    write_commands[i++] = rds_hsetnx;

    i = 0;
    read_commands[i++] = rds_hexists;
    read_commands[i++] = rds_hget;
    read_commands[i++] = rds_hgetall;
    read_commands[i++] = rds_hkeys;
    read_commands[i++] = rds_hlen;
    read_commands[i++] = rds_hmget;
    read_commands[i++] = rds_hvals;
    //read_commands[i++] = rds_hscan;
}
