#ifndef __JIMDB_CLIENT_H__
#define __JIMDB_CLIENT_H__

#include <stddef.h>
#include <stdint.h>

#include "jimdb_macro.h"

#ifdef __cplusplus
extern "C" {
#endif

//export c struct
typedef struct jimkv_reply_s {
    int                     type;       /* JIMDB_REPLY_* */
    int                     len;        /* Length of string */
    char                    *str;        /* Used for both JIMDB_REPLY_ERROR and JIMDB_REPLY_STRING */
    int64_t                 integer;    /* The integer when type is JIMDB_REPLY_INTEGER */
    size_t                  elements;   /* number of elements, for JIMDB_REPLY_ARRAY */
    struct jimkv_reply_s   **element;    /* elements vector for JIMDB_REPLY_ARRAY */
} jimkv_reply_t;

typedef struct jimkv_space_option_t jimkv_space_option_t;

extern void *jimkv_client_add_space(void *context, const jimkv_space_option_t *option);

extern int jimkv_client_destroy_space(void *cluster);

extern int jimkv_client_start(void* context);

extern void *jimkv_client_create(int use_vma, int thread_num);

extern void jimkv_client_destroy(void *context);

extern jimkv_space_option_t* jimkv_space_option_create(const char* master,
        uint64_t cluster_id, uint64_t db_id, uint64_t table_id, uint32_t conn_count);

extern void jimkv_space_option_destroy(jimkv_space_option_t*);

// sync interface
extern jimkv_reply_t *jimkv_command_exec(void *context, int argc,
        const char **argv, const size_t *argv_len, int timeout);

extern void jimkv_reply_free(void *reply);

extern void *jimkv_reply_error(const char *err, ...) __attribute__((format(printf, 1, 2)));
extern void *jimkv_reply_string(int type, const char *str, size_t len);

extern void *jimkv_pipe_command_create(void *context, int timeout);

extern int jimkv_pipe_command_append(void *pipe_context, int argc,
        const char **argv, const size_t *argv_len);

extern jimkv_reply_t **jimkv_pipe_command_exec(size_t *reply_num, void *pipe_context);

extern void jimkv_pipe_command_cancel(void *pipe_context);

extern void jimkv_pipe_reply_free(jimkv_reply_t **reply, size_t reply_num);

// async interface
typedef void (*jimkv_async_single_cb_t) (jimkv_reply_t *reply, void *user_date);
typedef void (*jimkv_async_pipe_cb_t) (jimkv_reply_t **reply,
        size_t reply_num, void *user_date);

extern void jimkv_async_cmd_exec(void *space_context, int argc,
        const char **argv, const size_t *argv_len, int timeout,
        jimkv_async_single_cb_t callback, void *data);

extern void *jimkv_async_pipe_cmd_create(void *space_context, int timeout,
        jimkv_async_pipe_cb_t callback, void *data);

extern int jimkv_async_pipe_cmd_append(void *pipe_context, int argc,
        const char **argv, const size_t *argv_len);

extern void jimkv_async_pipe_cmd_exec(void *pipe_context);

#ifdef __cplusplus
}
#endif

#endif//__JIMDB_CLIENT_H__
