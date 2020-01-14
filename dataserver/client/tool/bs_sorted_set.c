#include "batch_stress.h"

#define SORTED_SET_WRITE_CMD_SIZE  2
#define SORTED_SET_READ_CMD_SIZE   8

#define LIST_WAIT_MAX_TIME   60

static bs_command_call_t write_commands[SORTED_SET_WRITE_CMD_SIZE];
static bs_command_call_t read_commands[SORTED_SET_READ_CMD_SIZE];

void sorted_set_command(char *key, unsigned int *seed, int key_code, int mode) {
    bs_command_call_t call;

    int hash_code = rand_r(seed);

    if (mode == BS_WRITE_COMMAND) {
        call = write_commands[hash_code % SORTED_SET_WRITE_CMD_SIZE];
    } else {
        call = read_commands[hash_code % SORTED_SET_READ_CMD_SIZE];
    }

    return call(key, seed);
}

static void rds_sadd(char *key, unsigned int *seed) {
    cmd_argv_repeat("sadd", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_scard(char *key, unsigned int *seed) {
    cmd_argc_2("scard", key);
}

static void rds_sdiff(char *key, unsigned int *seed) {
    cmd_argv_repeat("sdiff", key, seed, BS_REPEAT_KEY, 0, 0);
}

static void rds_sdiffstore(char *key, unsigned int *seed) {
    cmd_argv_repeat("sdiffstroe", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_sinter(char *key, unsigned int *seed) {
    cmd_argv_repeat("sinter", key, seed, BS_REPEAT_KEY, 0, 0);
}

static void rds_sinterstore(char *key, unsigned int *seed) {
    cmd_argv_repeat("sinterstroe", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_sismember(char *key, unsigned int *seed) {
    int mem_len = rand_r(seed) % BS_KEY_MAX_LEN;
    cmd_argc_3("sismember", key, gen_rand_string(&mem_len, seed, BS_KEY));
}

static void rds_smembers(char *key, unsigned int *seed) {
    cmd_argc_2("rds_smembers", key);
}


static void rds_smove(char *key, unsigned int *seed) {
    int desc_len = rand_r(seed) % BS_KEY_MAX_LEN;
    int member_len = rand_r(seed) % BS_KEY_MAX_LEN;
    char *desc = gen_rand_string(&desc_len, seed, BS_KEY);
    char *member = gen_rand_string(&member_len, seed, BS_KEY);
    cmd_argc_4("smove", key, desc, member);
}

static void rds_spop(char *key, unsigned int *seed) {
    cmd_argc_2("spop", key);
}

static void rds_srandmember(char *key, unsigned int *seed) {
    char buf[64];
    int code = rand_r(seed);

    if (code % 2) {
        if (code % 13 % 2) {
            sprintf(buf, "-%d", code % 100);
        } else {
            sprintf(buf, "%d", code % 100);
        }
        cmd_argc_3("srandmember", key, strdup(buf));
    } else {
        cmd_argc_2("srandmember", key);
    }
}

static void rds_srem(char *key, unsigned int *seed) {
    cmd_argv_repeat("srem", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_sunion(char *key, unsigned int *seed) {
    cmd_argv_repeat("sunion", key, seed, BS_REPEAT_KEY, 0, 0);
}

static void rds_sunionstore(char *key, unsigned int *seed) {
    cmd_argv_repeat("sunionstore", key, seed, BS_REPEAT_KEY, 1, 0);
}

static void rds_hscan(char *key, unsigned int *seed) {
    cmd_scan("sscan", key, seed);
}

void list_init_() {
    int i = 0;

    write_commands[i++] = rds_sadd;
    //write_commands[i++] = rds_sdiffstore;
    //write_commands[i++] = rds_sinterstore;
    //write_commands[i++] = rds_smove;
    write_commands[i++] = rds_srem;
    //write_commands[i++] = rds_sunionstore;

    i = 0;
    read_commands[i++] = rds_scard;
    //read_commands[i++] = rds_sdiff;
    read_commands[i++] = rds_sinter;
    read_commands[i++] = rds_sismember;
    read_commands[i++] = rds_smembers;
    read_commands[i++] = rds_spop;
    read_commands[i++] = rds_srandmember;
    read_commands[i++] = rds_sunion;
    read_commands[i++] = rds_hscan;
}
