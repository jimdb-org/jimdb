#ifndef __JIMDB_BATCH_STRESS_H__
#define __JIMDB_BATCH_STRESS_H__

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

#define BS_COMMAND_TYPE_COUNT           1

#define BS_READ_COMMAND                 0
#define BS_WRITE_COMMAND                1

#define BS_COMMAND_KEYS                 0
#define BS_COMMAND_STRING               1
#define BS_COMMAND_HASH                 2
#define BS_COMMAND_LIST                 3
#define BS_COMMAND_SET                  4
#define BS_COMMAND_SORTED_SET           5
#define BS_COMMAND_HYPER_LOG_LOG        6

#define BS_KEY                          0
#define BS_VAL                          1

#define BS_KEY_MAX_LEN                  32
#define BS_VAL_MAX_LEN                  1024

#define BS_STR_DEFAULT_LEN              32

#define BS_MULTI_MAX_SIZE               5

#define BS_DAY_SECOND                   86400
#define BS_DAY_MILLISECOND              86400000

#define BS_REPEAT_KEY                   1   // 2^0
#define BS_REPEAT_VAL                   2   // 2^1

typedef void (*bs_command_call_t)(char*, unsigned int *);
typedef void (*bs_type_call_t)(char*, unsigned int *, int, int);

void keys_init();
void string_init();
void batch_hash_init();
void list_init();
void set_init();
void sorted_set_init();
void hyper_log_log_init();

void keys_command(char *key, unsigned int *seed, int key_code, int mode);
void string_command(char *key, unsigned int *seed, int key_code, int mode);
void hash_command(char *key, unsigned int *seed, int key_code, int mode);
void list_command(char *key, unsigned int *seed, int key_code, int mode);
void set_command(char *key, unsigned int *seed, int key_code, int mode);
void sorted_set_command(char *key, unsigned int *seed, int key_code, int mode);
void hyper_log_log_command(char *key, unsigned int *seed, int key_code, int mode);

void cmd_argc_1(const char *command);
void cmd_argc_2(const char *command, char *key);
void cmd_argc_3(const char *command, char *key, char *val);
void cmd_argc_4(const char *command, char *key, char *argv1, char *argv2);
void cmd_argv_repeat(const char *command, char *key, unsigned int *seed,
        int repeat_flat, int supply_val, int timeout);
void cmd_scan(const char *command, char *key, unsigned int *seed);
void cmd_range(const char *command, char *key, unsigned int *seed);

uint16_t mur_hash(char *key, int len);

char *gen_pattern_string(int *length, unsigned int *seed);
char *gen_rand_string(int *length, unsigned int *seed, int type);
//void read_reply(jimkv_reply_t *rep);
void print_reply(void *response, int argc, char** argv);
extern uint64_t g_req_number;
extern uint64_t g_req_time; //unit is seconds
extern void *cc;
extern void *space_context;
#endif//__JIMDB_BATCH_STRESS_H__
