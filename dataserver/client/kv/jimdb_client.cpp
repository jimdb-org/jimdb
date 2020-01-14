#include "jimdb_client.h"

#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#include <vector>
#include <memory>

#include "jim_kv_context.h"
#include "jim_kv_common.h"
#include "jim_kv_reply.h"
#include "jim_log.h"
#include "jim_table_client.h"
#include "jim_kv_command.h"

//using jim::sdk::kv::JimKVContext;
//using jim::sdk::kv::JimKVCommand;
//using jim::sdk::kv::JimTableClient;
//using jim::sdk::kv::KvCmdPtr;
//using jim::sdk::kv::KVReplyPtr;
//using jim::sdk::kv::TableClientPtr;
using namespace jim::sdk::kv;
using namespace jim::sdk;
extern "C" {

struct kv_reply_t {
    jimkv_reply_t           reply;
    KVReplyPtr              kv_reply;
};

struct jimkv_space_option_t {
    JimKVOption kv_option;
};

typedef struct async_single_context_t {
    void *user_data;
    jimkv_async_single_cb_t callback;
} async_single_context_t;

typedef struct async_pipe_context_t {
    void *user_data;
    jimkv_async_pipe_cb_t   callback;
} async_pipe_context_t;

typedef struct jimkv_context {
    JimKVContext& context = JimKVContext::Instance();
} jimkv_context_t;

typedef struct jimkv_space_context {
    jim::sdk::kv::TableClientPtr table_client;
} space_context_t;

typedef struct pipe_command_t {
    TableClientPtr table_client;
    KvCmdPtr jimkv_cmd;
    int timeout;
} jimkv_pipe_command_t;

void *jimkv_client_create(int use_vma, int thread_num) {
    JimKVContext::Instance().Start(thread_num);
    return static_cast<void*>(new jimkv_context_t);

}

void jimkv_client_destroy(void *context) {
    jimkv_context_t* client = static_cast<jimkv_context_t*>(context);
    delete client;
}

void *jimkv_client_add_space(void *context, const jimkv_space_option_t *option) {
    TableClientPtr client = JimKVContext::Instance().AddTable(option->kv_option);
    if (client == nullptr) {
        CBLOG_ERROR("add space failed");
        return NULL;
    }
    space_context_t *space = new space_context_t;
    space->table_client = client;
    return static_cast<void*>(space);
}

int jimkv_client_destroy_space(void *cluster) {
    return 0;
}

int jimkv_client_start(void* context) {
    return 0;
}

static void kv_reply_to_jimkv_reply(KVReplyPtr kv_reply, jimkv_reply_t **reply) {
    kv_reply_t *tmp;
    tmp = new kv_reply_t;
    if (tmp == nullptr) {
        CBLOG_ERROR("malloc failed, error: %s", strerror(errno));
        return;
    }
    switch (kv_reply->type()) {
    case JimKVReplyType::REPLY_INTEGER:
        tmp->reply.type = JIMDB_REPLY_INTEGER;
        tmp->reply.integer = kv_reply->integer();
        tmp->reply.elements = 0;
        break;
    case JimKVReplyType::REPLY_ERROR:
        tmp->reply.type = JIMDB_REPLY_ERROR;
        tmp->reply.len = kv_reply->buf().size();
        tmp->reply.str = (char*)kv_reply->buf().c_str();
        tmp->reply.elements = 0;
        break;
    case JimKVReplyType::REPLY_STRING:
        tmp->reply.type = JIMDB_REPLY_STRING;
        tmp->reply.len = kv_reply->buf().size();
        tmp->reply.str = (char*)kv_reply->buf().c_str();
        tmp->reply.elements = 0;
        break;
    case JimKVReplyType::REPLY_STATUS:
        tmp->reply.type = JIMDB_REPLY_STATUS;
        tmp->reply.len = kv_reply->buf().size();
        tmp->reply.str = (char*)kv_reply->buf().c_str();
        tmp->reply.elements = 0;
        break;
    case JimKVReplyType::REPLY_NIL:
        tmp->reply.type = JIMDB_REPLY_NIL;
        tmp->reply.len = 0;
        tmp->reply.str = NULL;
        tmp->reply.elements = 0;
        break;
    case JimKVReplyType::REPLY_ARRAY:
    {
        tmp->reply.type = JIMDB_REPLY_ARRAY;
        tmp->reply.len = 0;
        tmp->reply.str = NULL;
        tmp->reply.elements = kv_reply->element_size();
        tmp->reply.element = (jimkv_reply_t**)(new kv_reply_t*[tmp->reply.elements]);
        auto elements = kv_reply->elements();
        int i = 0;
        for (KVReplyPtr it_reply: elements) {
            kv_reply_to_jimkv_reply(it_reply, &tmp->reply.element[i]);
            i++;
        }
        break;
    }
    case JimKVReplyType::REPLY_MAX: /* fall-thru*/
    default:
        CBLOG_ERROR("not expected reply type: %d", kv_reply->type());
        break;
    }
    tmp->kv_reply = kv_reply;
    *reply = (jimkv_reply_t*)tmp;
}

jimkv_reply_t *jimkv_command_exec(void *context, int argc,
        const char **argv, const size_t *argv_len, int timeout)
{
    space_context_t *space_context = static_cast<space_context_t*>(context);
    std::vector<std::string> args;
    for (int i = 0; i<argc; i++) {
        args.emplace_back(argv[i], argv_len[i]);
    }
    KVReplyPtr kv_reply;
    auto status = space_context->table_client->CommandExec(args, timeout, kv_reply);
    if (status != JimStatus::OK) {
        CBLOG_ERROR("exec command failed");
        return NULL;
    }
    jimkv_reply_t *reply = nullptr;
    kv_reply_to_jimkv_reply(kv_reply, &reply);
    return reply;
}

static void jimkv_reply_del(jimkv_reply_t *reply) {
    size_t j;

    switch (reply->type) {
    case JIMDB_REPLY_INTEGER: break; /* Nothing to free */
    case JIMDB_REPLY_ARRAY:
        for (j = 0; j < reply->elements; j++) {
            jimkv_reply_del(reply->element[j]);
        }
        delete[] (kv_reply_t**)(reply->element);
        break;
    case JIMDB_REPLY_ERROR:     /* fall-thru */
    case JIMDB_REPLY_STATUS:    /* fall-thru */
    case JIMDB_REPLY_NIL:       /* fall-thru */
    case JIMDB_REPLY_STRING:
        break;
    }
    delete (kv_reply_t*)reply;
}

void jimkv_reply_free(void *reply) {
    jimkv_reply_t *rep = static_cast<jimkv_reply_t*>(reply);
    jimkv_reply_del(rep);
}

void *jimkv_pipe_command_create(void *context, int timeout) {
    KvCmdPtr cmd = nullptr;
    jimkv_pipe_command_t *pipe_command = NULL;
    space_context_t *space = static_cast<space_context_t*>(context);

    cmd = space->table_client->PipeCommandCreate();
    pipe_command = new jimkv_pipe_command_t;
    pipe_command->timeout = timeout;
    pipe_command->jimkv_cmd = cmd;
    pipe_command->table_client = space->table_client;
    return static_cast<void*>(pipe_command);
}

void jimkv_pipe_command_cancel(void *pipe_context) {
    jimkv_pipe_command_t *pipe_command = static_cast<jimkv_pipe_command_t*>(pipe_context);
    delete pipe_command;
}

int jimkv_pipe_command_append(void *pipe_context, int argc,
        const char **argv, const size_t *argv_len)
{
    jimkv_pipe_command_t *pipe_command = static_cast<jimkv_pipe_command_t*>(pipe_context);
    ArrayString args;
    for (int i = 0; i < argc; i++) {
        args.emplace_back(argv[i], argv_len[i]);
    }
    auto status = pipe_command->jimkv_cmd->append_sub_command(args);
    if (status != JimStatus::OK) {
        CBLOG_ERROR("append command failed");
        return JIMDB_ERR;
    }
    return JIMDB_OK;
}

jimkv_reply_t **jimkv_pipe_command_exec(size_t *reply_num, void *pipe_context) {
    jimkv_reply_t **jim_reply = NULL;
    jimkv_pipe_command_t *pipe_command = static_cast<jimkv_pipe_command_t*>(pipe_context);
    ArrayKVReplyPtr replies;
    //todo fix timeout
    auto status = pipe_command->table_client->PipeCommandExec(pipe_command->jimkv_cmd, replies, pipe_command->timeout);
    if (status != JimStatus::OK) {
        CBLOG_ERROR("pipe command execute failed");
        delete pipe_command;
        return NULL;
    }
    jim_reply = new jimkv_reply_t*[replies.size()];
    int i = 0;
    for (KVReplyPtr kv_reply: replies) {
        kv_reply_to_jimkv_reply(kv_reply, &jim_reply[i]);
        i++;
    }

    *reply_num = replies.size();

    delete pipe_command;

    return jim_reply;
}

void jimkv_pipe_reply_free(jimkv_reply_t **reply, size_t reply_num) {
    for (size_t i = 0; i < reply_num; i++) {
        jimkv_reply_free(reply[i]);
    }
    delete[] reply;
}

static void async_single_callback(const ArrayVoidPtr& replies, VoidPtr user_data) {
    jimkv_reply_t *reply;
    auto async_context = std::static_pointer_cast<async_single_context_t>(user_data);
    if (replies.size() != 1) {
        CBLOG_WARN("invalid reply, reply number: %zu", replies.size());
    }

    KVReplyPtr kv_reply = std::static_pointer_cast<JimKVReply>(replies[0]);

    kv_reply_to_jimkv_reply(kv_reply, &reply);
    async_context->callback(reply, async_context->user_data);
}

static void async_pipe_callback(const ArrayVoidPtr& replies, VoidPtr user_data) {
    jimkv_reply_t **reply;
    auto async_context = std::static_pointer_cast<async_pipe_context_t>(user_data);
    reply = new jimkv_reply_t*[replies.size()];
    int i = 0;
    for (VoidPtr kv_reply: replies) {
        kv_reply_to_jimkv_reply(std::static_pointer_cast<JimKVReply>(kv_reply), &reply[i]);
        i++;
    }
    async_context->callback(reply, replies.size(), async_context->user_data);
}

//async interface
void jimkv_async_cmd_exec(void *space_context, int argc,
                          const char **argv, const size_t *argv_len, int timeout,
                          jimkv_async_single_cb_t callback, void *data)
{
    space_context_t *context = static_cast<space_context_t*>(space_context);
    std::vector<std::string> args;
    for (int i = 0; i<argc; i++) {
        args.push_back(argv[i]);
    }
    KVReplyPtr kv_reply;
    auto async_context = std::make_shared<async_single_context_t>();
    async_context->callback = callback;
    async_context->user_data = data;

    auto status = context->table_client->CommandExec(args, timeout, kv_reply, async_single_callback,
                                                           std::static_pointer_cast<void>(async_context));
    if (status != JimStatus::OK) {
        CBLOG_ERROR("exec command failed");
        if (callback != nullptr) {
            jimkv_reply_t *reply;
            kv_reply = NewJimKVReply();
            kv_reply->set_type(JimKVReplyType::REPLY_ERROR);
            kv_reply->set_buf("exec command failed");
            kv_reply_to_jimkv_reply(kv_reply, &reply);
            callback(reply, data);
        }
    }
}

void *jimkv_async_pipe_cmd_create(void *space_context, int timeout,
        jimkv_async_pipe_cb_t callback, void *data)
{

    KvCmdPtr cmd = nullptr;
    jimkv_pipe_command_t *pipe_command = NULL;
    space_context_t *space = static_cast<space_context_t*>(space_context);
    auto async_context = std::make_shared<async_pipe_context_t>();
    async_context->callback = callback;
    async_context->user_data = data;

    cmd = space->table_client->PipeCommandCreate(async_pipe_callback, async_context);
    pipe_command = new jimkv_pipe_command_t;
    pipe_command->timeout = timeout;
    pipe_command->jimkv_cmd = cmd;
    pipe_command->table_client = space->table_client;
    return static_cast<void*>(pipe_command);
}

int jimkv_async_pipe_cmd_append(void *pipe_context, int argc,
        const char **argv, const size_t *argv_len)
{
    jimkv_pipe_command_t *pipe_command = static_cast<jimkv_pipe_command_t*>(pipe_context);
    ArrayString args;
    for (int i = 0; i < argc; i++) {
        args.emplace_back(argv[i], argv_len[i]);
    }
    auto status = pipe_command->jimkv_cmd->append_sub_command(args);
    if (status != JimStatus::OK) {
        CBLOG_ERROR("append command failed, status: %d", status);
        return JIMDB_ERR;
    }
    return JIMDB_OK;
}

void jimkv_async_pipe_cmd_exec(void *pipe_context) {
    jimkv_reply_t **jim_reply = NULL;
    jimkv_pipe_command_t *pipe_command = static_cast<jimkv_pipe_command_t*>(pipe_context);
    ArrayKVReplyPtr replies;
    //todo fix timeout
    auto status = pipe_command->table_client->PipeCommandExec(pipe_command->jimkv_cmd, replies, pipe_command->timeout);
    if (status != JimStatus::OK) {
        CBLOG_ERROR("pipe command execute failed");
    }
    delete pipe_command;
}

jimkv_space_option_t* jimkv_space_option_create(const char* master,
                       uint64_t cluster_id, uint64_t db_id, uint64_t table_id, uint32_t conn_count)
{
    auto option = new jimkv_space_option_t;
    option->kv_option.master_address = std::string(master);
    option->kv_option.cluster_id = cluster_id;
    option->kv_option.db_id = db_id;
    option->kv_option.table_id = table_id;
    option->kv_option.conn_count = conn_count;
    return option;
}

void jimkv_space_option_destroy(jimkv_space_option_t* option) {
    delete option;
}

static jimkv_reply_t *jimkv_reply_create(int type) {
    kv_reply_t *kv_reply = new kv_reply_t;
    if (kv_reply == nullptr) {
        CBLOG_ERROR("malloc fail, error: %s.", strerror(errno));
        return NULL;
    }
    kv_reply->reply.type = type;
    return (jimkv_reply_t*)kv_reply;
}

void *jimkv_reply_string(int type, const char *str, size_t len) {
    jimkv_reply_t * r = jimkv_reply_create(type);
    if (r == NULL) {
        return NULL;
    }

    char *buf = (char*)malloc(len + 1);
    if (buf == NULL) {
        CBLOG_ERROR("malloc size:%ld fail.", len + 1);
        jimkv_reply_free(r);
        return NULL;
    }

    memcpy(buf, str, len);
    buf[len] = '\0';

    r->str = buf;
    r->len = len;

    return r;
}

void *jimkv_reply_error(const char *err, ...) {
    char err_buf[256];
    int err_len;

    va_list ap;
    va_start(ap, err);
    err_len = vsnprintf(err_buf, sizeof(err_buf), err, ap);
    va_end(ap);

    return jimkv_reply_string(JIMDB_REPLY_ERROR, err_buf, err_len);
}
} // end extern "C"
