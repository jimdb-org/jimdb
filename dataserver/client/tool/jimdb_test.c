#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

#include "jimdb_client.h"
#include "jimdb_macro.h"

void sdk_reply_callback(jimkv_reply_t *rep, void *cb_handle) {
    switch (rep->type) {
    case JIMDB_REPLY_NIL:
        fprintf(stderr, "result is nil\n");
        break;
    case JIMDB_REPLY_STRING:
        fprintf(stderr, "result is %.*s\n", rep->len, rep->str);
        break;
    case JIMDB_REPLY_INTEGER:
        fprintf(stderr, "result is integer: %lu\n", rep->integer);
        break;
    case JIMDB_REPLY_ERROR:
        fprintf(stderr, "result is error: %.*s\n", rep->len, rep->str);
        break;
    case JIMDB_REPLY_ARRAY:
        fprintf(stderr, "result is array\n");
        break;
    case JIMDB_REPLY_STATUS:
        fprintf(stderr, "result is status\n");
        break;
    default:
        fprintf(stderr, "not expected reply type: %d\n", rep->type);
        break;
    }
    return;
}

int main(int argc, char **argv)
{
    void *cc_handle = NULL;
    void *space_context = NULL;

    jimkv_space_option_t *opt = jimkv_space_option_create("11.3.90.194:443", 100, 1, 2, 2);

    if ((cc_handle = jimkv_client_create(0, 1)) == NULL) {
        exit(1);
    }
    jimkv_client_start(cc_handle);
    space_context = jimkv_client_add_space(cc_handle, opt);
    if (space_context == NULL) {
        exit(1);
    }

    jimkv_space_option_destroy(opt);

    char *set_cmd[5];
    size_t set_cmd_len[5];

    set_cmd[0] = malloc(20);
    set_cmd[1] = malloc(50);
    set_cmd[2] = malloc(50);
    set_cmd[3] = malloc(50);
    set_cmd[4] = malloc(50);

    strcpy(set_cmd[0], "mset");
    set_cmd_len[0] = 4;

    strcpy(set_cmd[1], "jacky");
    set_cmd_len[1] = strlen(set_cmd[1]);

    strcpy(set_cmd[2], "157_fe440baad545459c8e59148c9c7c6fef_1");
    set_cmd_len[2] = strlen(set_cmd[2]);

    strcpy(set_cmd[3], "candy");
    set_cmd_len[3] = strlen(set_cmd[3]);

    strcpy(set_cmd[4], "157_fe440baad545459c8e59148c9c7c6fef_1");
    set_cmd_len[4] = strlen(set_cmd[4]);

    jimkv_reply_t *set_rep = jimkv_command_exec(space_context,
            5, (const char **)set_cmd, set_cmd_len, 30);
    if (set_rep != NULL) {
        fprintf(stderr, "reply type: %d\n", set_rep->type);
        jimkv_reply_free(set_rep);
    }

    char *get_cmd[3];
    size_t get_cmd_len[3];

    get_cmd[0] = malloc(20);
    get_cmd[1] = malloc(20);
    get_cmd[2] = malloc(20);

    strcpy(get_cmd[0], "mget");
    get_cmd_len[0] = 4;
    strcpy(get_cmd[1], "jacky");
    get_cmd_len[1] = strlen(get_cmd[1]);
    strcpy(get_cmd[2], "candy");
    get_cmd_len[2] = strlen(get_cmd[2]);

    jimkv_reply_t *get_rep = jimkv_command_exec(space_context,
            3, (const char **)get_cmd, get_cmd_len, 30);
    if (get_rep != NULL) {
        fprintf(stderr, "reply type: %d\n", get_rep->type);
        jimkv_reply_free(get_rep);
    }

    jimkv_async_cmd_exec(space_context, 3, (const char**)get_cmd, get_cmd_len, 30, sdk_reply_callback, NULL);
    sleep(2);
    return 0;
}
