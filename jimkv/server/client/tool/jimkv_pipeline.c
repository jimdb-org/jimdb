#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "jimkv_client.h"
#include "jimkv_log.h"

size_t set_cmd_len[3] = {3, 1, 1};
size_t get_cmd_len[2] = {3, 1};
size_t del_cmd_len[5] = {3, 1, 1, 1, 1};
//size_t del_cmd_len[2] = {3, 1};
long long right_all = 0, wrong_all = 0, cmd_totle = 0;

void print_reply(jimkv_reply_t *_rep) {
    size_t i = 0;
    jimkv_reply_t *rep = _rep;
    if (rep) {
        switch (rep->type) {
            case JIMDB_REPLY_STATUS:
                CBLOG_INFO("reply is status");
                break;
            case JIMDB_REPLY_STRING:
                {
                    CBLOG_INFO("reply is string, result is %s", rep->str);
                }
                break;
            case JIMDB_REPLY_ERROR:
                CBLOG_INFO("reply is error, buf is %s", rep->str);
                break;
            case JIMDB_REPLY_INTEGER:
                CBLOG_INFO("reply is integer, result is %"PRId64, rep->integer);
                break;
            case JIMDB_REPLY_NIL:
                CBLOG_INFO("reply is nil");
                break;
            case JIMDB_REPLY_ARRAY:
                {
                    CBLOG_INFO("reply is array");
                    for (i = 0; i < rep->elements; i++)
                        print_reply(rep->element[i]);
                }
                break;
            default:
                CBLOG_ERROR("invalid type: %d of reply", rep->type);
                break;
        }
    } else {
        CBLOG_ERROR("reply is NULL, invalid");
    }
}

void sdk_pipe_reply_callback(jimkv_reply_t **rep, size_t rep_num, void *cb_handle) {
    size_t i = 0;
    jimkv_reply_t *reply = NULL;
    for (i = 0; i < rep_num; i++) {
        reply = rep[i];
        if (reply != NULL) {
            print_reply(reply);
        } else {
            CBLOG_ERROR("reply is null, i: %zd", i);
        }
    }
    jimkv_pipe_reply_free(rep, rep_num);
    if (cb_handle) {
        free(cb_handle);
    }
}

void sdk_reply_callback(jimkv_reply_t *rep, void *cb_handle) {
    print_reply(rep);
    jimkv_reply_free(rep);
    if (cb_handle) {
        free(cb_handle);
    }
}

int main(int argc, char **argv) {
    jimkv_log_level_set(JIMKV_LOG_DEBUG);
    CBLOG_INFO( "----------------------getset start-------------------");
    void *cc_handle = NULL;
    void *space_context = NULL;
    
    jimkv_space_option_t *opt = jimkv_space_option_create("11.3.90.194:443", 100, 1, 2, 8);

    if ((cc_handle = jimkv_client_create(0, 8)) == NULL) {
        CBLOG_ERROR("init space error");
        exit(1);
    }

    space_context = jimkv_client_add_space(cc_handle, opt);
    if (space_context == NULL) {
        CBLOG_ERROR("add space failed");
        exit(1);
    }
    jimkv_space_option_destroy(opt);
    int ret = jimkv_client_start(cc_handle);
    if (ret != JIMDB_OK) {
        CBLOG_ERROR("client start failed");
        exit(1);
    }
    CBLOG_WARN("=========================");
    int i, total, threadid = 0;
    total = 1;

    char *set_cmd[4][3] = {
        {"set", "1", "1"},
        {"set", "2", "2"},
        {"set", "3", "3"},
        {"set", "4", "4"}
    };

    char *get_cmd[4][2] = {
        {"get", "1"},
        {"get", "2"},
        {"get", "3"},
        {"get", "4"}
    };

    char *del_cmd[1][5] = {
        {"del", "1", "2", "3", "4"}
    };

    for (i = 0; i < total; i++) {
        /*char *del_cmd[4][2] = {
                {"del", "1"},
                {"del", "2"},
                {"del", "3"},
                {"del", "4"}};*/


        /*char *del_cmd[1][2] = {
                {"del", "1"}
        };*/

        jimkv_reply_t **reply = NULL;
        if (cc_handle == NULL) {
            CBLOG_ERROR("context is null");
            exit(-1);
        }

        void *pipe_handle = jimkv_async_pipe_cmd_create(space_context, 1000, sdk_pipe_reply_callback, NULL);
        size_t set_cmd_len[3] = {3, 1, 1};
        if (jimkv_async_pipe_cmd_append(pipe_handle, 3, (const char **) set_cmd[0], set_cmd_len) == -1) {
            CBLOG_ERROR("jimkv_async_pipe_cmd_append error");
            return -1;
        }

        size_t get_cmd_len[2] = {3, 1};
        if (jimkv_async_pipe_cmd_append(pipe_handle, 2, (const char **) get_cmd[0], get_cmd_len) == -1) {
            CBLOG_ERROR("jimkv_async_pipe_cmd_append error");
            return -1;
        }

        jimkv_async_pipe_cmd_exec(pipe_handle);

    }//end circle
    sleep(30);
    return 0;
}
