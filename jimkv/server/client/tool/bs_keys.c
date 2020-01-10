#include <stdio.h>
#include <time.h>
#include "batch_stress.h"

#define KEY_WRITE_CMD_SIZE  6
#define KEY_READ_CMD_SIZE   6

static bs_command_call_t keys_write_commands[KEY_WRITE_CMD_SIZE];
static bs_command_call_t keys_read_commands[KEY_READ_CMD_SIZE];

void keys_command(char *key, unsigned int *seed, int key_code, int mode) {
    bs_command_call_t call;

    int hash_code = rand_r(seed);

    if (mode == BS_WRITE_COMMAND) {
        call = keys_write_commands[hash_code % KEY_WRITE_CMD_SIZE];
    } else {
        call = keys_read_commands[hash_code % KEY_READ_CMD_SIZE];
    }

    return call(key, seed);
}
static void rds_del(char *key, unsigned int *seed) {
    cmd_argv_repeat("del", key, seed, 0, 0, 0);
}
static void rds_dump(char *key, unsigned int *seed) {
    cmd_argc_2("dump", key);
}

static void rds_exists(char *key, unsigned int *seed) {
    cmd_argc_2("exists", key);
}

static void rds_expire(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%d", rand_r(seed) % BS_DAY_SECOND);
    cmd_argc_3("expire", key, strdup(buf));
}
static void rds_expireat(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%ld", time(NULL) + rand_r(seed) % BS_DAY_SECOND);
    cmd_argc_3("expireat", key, strdup(buf));
}
/*
static void rds_keys(char *key, unsigned int *seed) {
    int pattern_len = 0;
    cmd_argc_2("keys", gen_pattern_string(&pattern_len, seed));
}
*/

static void rds_object(char *key, unsigned int *seed) {
    static char *subcommand[] = {"refcount", "encoding", "idletime"};
    char *sub = subcommand[rand_r(seed) % 3];
    cmd_argc_3("object", strdup(sub), key);
}

static void rds_persist(char *key, unsigned int *seed) {
    cmd_argc_2("persist", key);
}
static void rds_pexpire(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%d", rand_r(seed) % BS_DAY_MILLISECOND);
    cmd_argc_3("pexpire", key, strdup(buf));
}

static void rds_pexpireat(char *key, unsigned int *seed) {
    char buf[64];
    sprintf(buf, "%ld", time(NULL) * 1000 + rand_r(seed) % BS_DAY_MILLISECOND);
    cmd_argc_3("pexpireat", key, strdup(buf));
}
static void rds_pttl(char *key, unsigned int *seed) {
    cmd_argc_2("pttl", key);
}

static void rds_randomkey(char *key, unsigned int *seed) {
    cmd_argc_1("randomkey");
}
/*
static void rds_rename(char *key, unsigned int *seed) {
    int len = 0;
    cmd_argc_3("rename", key, gen_pattern_string(&len, seed));
}

static void rds_renamenx(char *key, unsigned int *seed) {
    int len = 0;
    cmd_argc_3("renamenx", key, gen_pattern_string(&len, seed));
}
*/
static void rds_ttl(char *key, unsigned int *seed) {
    cmd_argc_2("ttl", key);
}

static void rds_type(char *key, unsigned int *seed) {
    cmd_argc_2("type", key);
}
/*
static void rds_scan(char *key, unsigned int *seed) {
    cmd_scan("scan", NULL, seed);
}
*/
void keys_init() {
    int i = 0;

    keys_write_commands[i++] = rds_del;
    keys_write_commands[i++] = rds_expire;
    keys_write_commands[i++] = rds_expireat;
    keys_write_commands[i++] = rds_persist;
    keys_write_commands[i++] = rds_pexpire;
    keys_write_commands[i++] = rds_pexpireat;
    //keys_write_commands[i++] = rds_rename;
    //keys_write_commands[i++] = rds_renamenx;

    i = 0;
    keys_read_commands[i++] = rds_dump;
    keys_read_commands[i++] = rds_exists;
    //keys_read_commands[i++] = rds_keys;
    keys_read_commands[i++] = rds_object;
    keys_read_commands[i++] = rds_pttl;
    keys_read_commands[i++] = rds_ttl;
    keys_read_commands[i++] = rds_type;
    //keys_read_commands[i++] = rds_scan;
}
