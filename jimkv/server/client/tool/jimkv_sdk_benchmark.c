#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>

#include "jimkv_client.h"
#include "jimkv_log.h"


#define JIMDB_NOTUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8
#define CMD_MAX_LENGTH 2048
#define BS_STR_DEFAULT_LEN 32
#define SDK_RAND_KV_DEFAULT_SIZE 1000

int64_t exec_cmd_del(char *key);

typedef struct _jimkv_exec_cmd_t {
    size_t  *argv_len;
    char    ***argv;
    int     fields;
    int     cmds;
} jimkv_exec_cmd_t;

typedef struct _config {
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int num_clients;
    int sdk_threads;
    int live_clients;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keep_alive;
    int pipe_line;
    long long start;
    long long totlatency;
    long long *latency;
    const char *title;
    //jimkv_list_t clients;
    int quiet;
    int csv;
    int loop;
    int run_time;
    int idle_mode;
    int dbnum;
    char *dbnumstr;
    char *tests;
    char *auth;
    char *space_id;
    char *token;
    char *cfs_url;
    int conf_id;
    int vma;
    int cache_flag;
    //jimkv_list_t clearkeylist;
} config_t;

typedef struct _data {
    char *key;
    char *value;
} data_t;

typedef struct _jimkv_benchmark_context_t {
    config_t *cfg;
    jimkv_exec_cmd_t *cmd;
    char *cmd_line;
    data_t *data;
} jimkv_benchmark_context_t;

typedef struct _client {
    jimkv_benchmark_context_t *context;
    char *obuf;
    char **rand_ptr;         /* Pointers to :rand: strings inside the command buf */
    size_t rand_len;         /* Number of pointers in client->rand_ptr */
    size_t rand_free;        /* Number of unused pointers in client->rand_ptr */
    unsigned int written;   /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipe_line of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
//    jimkv_list_entry_t link;
} *client;

//typedef struct _key_obj_t {
//    char *key_str;
//    jimkv_list_entry_t link;
//} *key_obj_t;
//
//randomkey migrate from cold_hot_split

typedef struct _user_data {
    int idx;
    uint64_t start_time;
} user_data_t;

data_t *data;
unsigned int seed;
unsigned int sequence = 0;

pthread_mutex_t fin_mutex;
pthread_cond_t fin_cond;

static int data_size;
//static int data_number_len;
static int data_number;

static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
static int charset_len = sizeof(charset) - 1;

///////////////////////////////////////////////////////
static void  *cc_handle = NULL;
static void *space_context = NULL;
static config_t config;

//static jimkv_benchmark_context_t *jimkv_benchmark_context_create(jimkv_exec_cmd_t *cmd);
static void jimkv_context_destroy(jimkv_benchmark_context_t *ctx);
static void *func_cmd_consume(void *ctx_data);
static void bench(jimkv_benchmark_context_t *ctx);
static long long ustime(void);
jimkv_exec_cmd_t *jimkv_exec_cmd_create(config_t *cfg, const char *format, ...);
void jimkv_exec_cmd_destroy(jimkv_exec_cmd_t *cmd);

void sdk_pipe_reply_callback(jimkv_reply_t **rep, size_t rep_num, void *cb_handle) {
    user_data_t *user_data = cb_handle;
    long long latency = ustime() - user_data->start_time;
    config.latency[user_data->idx] = latency;
    __sync_fetch_and_add(&config.requests_finished, 1);
    if (__sync_bool_compare_and_swap(&config.requests_finished, config.requests,
                config.requests))
    {
        CBLOG_INFO("command finished");
        pthread_cond_signal(&fin_cond);
    }
    jimkv_pipe_reply_free(rep, rep_num);
    free(user_data);
}

void sdk_reply_callback(jimkv_reply_t *rep, void *cb_handle) {
    user_data_t *user_data = cb_handle;
    long long latency = ustime() - user_data->start_time;
    config.latency[user_data->idx] = latency;
    __sync_fetch_and_add(&config.requests_finished, 1);
    if (__sync_bool_compare_and_swap(&config.requests_finished, config.requests,
                config.requests))
    {
        CBLOG_INFO("command finished");
        pthread_cond_signal(&fin_cond);
    }
    jimkv_reply_free(rep);
    free(user_data);
}

char * gen_rand_string(int length, unsigned int *seed) {
    int n;
    char *random_str = NULL;
    int len = length;

    if (len <= 0) {
        len = BS_STR_DEFAULT_LEN;
    }

    random_str = malloc(sizeof(char) * (len + 1));
    if (random_str) {
        for (n = 0; n < len; n++) {
            random_str[n] = charset[rand_r(seed) % charset_len];
        }
        random_str[len] = '\0';
        return random_str;
    }
    return NULL;
}

char *gen_rand_key() {
    char * rand_key = NULL;
    rand_key = gen_rand_string(BS_STR_DEFAULT_LEN, &seed);
    sprintf(rand_key+10, "%06d", sequence++);
    return rand_key;
}

char *gen_rand_value() {
    return gen_rand_string(data_size, &seed);
}

int data_init() {
    int i;
    data_t *tmp_data;
    data = calloc(data_number, sizeof(data_t));
    if (!data) {
        CBLOG_ERROR("calloc data failed, error: %s", strerror(errno));
        return JIMDB_ERR;
    }
    for (i = 0; i < data_number; i++) {
        tmp_data = data+i;
        tmp_data->key = gen_rand_key();
        tmp_data->value = gen_rand_value();
        //CBLOG_INFO("key is %s, value is %s", tmp_data->key, tmp_data->value);
    }
    return JIMDB_OK;
}

void data_destroy() {
    int i;
    data_t *tmp_data;
    for (i = 0; i < data_number; i++) {
        tmp_data = data + i;
        free(tmp_data->key);
        free(tmp_data->value);
        tmp_data->key = NULL;
        tmp_data->value = NULL;
    }
    free(data);
}
char **split(char *src, const char seperator, const int nMaxCols, int *nColCount)
{
    char **pCols;
    char **pCurrent;
    char *p;
    int i;
    int nLastIndex;

    if (src == NULL)
    {
        *nColCount = 0;
        return NULL;
    }

    *nColCount = 1;
    p = strchr(src, seperator);

    while (p != NULL)
    {
        (*nColCount)++;
        p = strchr(p + 1, seperator);
    }

    if (nMaxCols > 0 && (*nColCount) > nMaxCols)
    {
        *nColCount = nMaxCols;
    }

    pCurrent = pCols = (char **)malloc(sizeof(char *) * (*nColCount));
    if (pCols == NULL)
    {
        CBLOG_ERROR("file: "__FILE__", line: %d, " \
                "malloc %d bytes fail", __LINE__, \
                (int)sizeof(char *) * (*nColCount));
        return NULL;
    }

    p = src;
    nLastIndex = *nColCount - 1;
    for (i=0; i<*nColCount; i++)
    {
        *pCurrent = p;
        pCurrent++;

        p = strchr(p, seperator);
        if (i != nLastIndex)
        {
            *p = '\0';
            p++;
        }
    }

    return pCols;
}

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    if (0 != gettimeofday(&tv, NULL)) {
        CBLOG_ERROR("gettimeofday error.");
    }
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void free_client(client c) {
    if (c != NULL) {
        //jimkv_list_erase(&config.clients, &c->link);
        free(c->context);
        free(c->obuf);
        free(c->rand_ptr);
        free(c);
    }
    config.live_clients--;
}

/*static void free_all_clients(void) {
  client c;
  client next;

  list_for_each_entry_safe(&config.clients, c, next, link) {
  free_client(c);
  }
  }*/

/*static void reset_client(client c) {
  c->written = 0;
  c->pending = config.pipe_line;
  }*/

/*static void client_done(client c) {
  if (config.requests_finished == config.requests) {
  free_client(c);
  return;
  }
  if (config.keep_alive) {
  reset_client(c);
  } else {
  config.live_clients--;
  config.live_clients++;
  free_client(c);
  }
  }*/

static client create_client_local() {
    client c = malloc(sizeof(struct _client));
    if (c == NULL) {
        fprintf(stderr,"Could not malloc memory,exit...");
        exit(1);
    }

    c->context = NULL;
    c->obuf = NULL;
    c->prefix_pending = 0;

    c->written = 0;
    c->pending = config.pipe_line+c->prefix_pending;
    c->rand_ptr = NULL;
    c->rand_len = 0;

    //if (config.randomkeys) {
    //    char *p = c->obuf;

    //    c->rand_len = 0;
    //    c->rand_free = RANDPTR_INITIAL_SIZE;
    //    c->rand_ptr = malloc(sizeof(char*)*c->rand_free);
    //    while ((p = strstr(p,"__rand_int__##########")) != NULL) {
    //        if (c->rand_free == 0) {
    //            c->rand_ptr = realloc(c->rand_ptr,sizeof(char*)*c->rand_len*2);
    //            c->rand_free += c->rand_len;
    //        }
    //        c->rand_ptr[c->rand_len++] = p;
    //        c->rand_free--;
    //        p += 12; /* 12 is strlen("__rand_int__##########). */
    //    }
    //}

    //jimkv_list_push_back(&config.clients, &c->link);
    config.live_clients++;
    return c;
}

static int compareLatency(const void *a, const void *b) {
    return (*(long long*)a)-(*(long long*)b);
}

static void showLatencyReport(void) {
    long long i, curlat = 0;
    float perc, reqpersec;
    pthread_mutex_lock(&fin_mutex);
    pthread_cond_wait(&fin_cond, &fin_mutex);
    reqpersec = (float)config.requests_finished/((float)config.totlatency/1000);
    if (!config.quiet && !config.csv) {
        //printf("====== %s ======\n", config.title);
        //printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
        //        (float)config.totlatency/1000);
        //printf("  %d clients threads\n", config.num_clients);
        //printf("  %d sdk threads\n", config.sdk_threads);
        //printf("  %d bytes payload\n", config.datasize);
        //printf("  %d pipeline\n", config.pipe_line);
        //printf("  keep alive: %d\n", config.keep_alive);
        //printf("\n");

        CBLOG_INFO("====== %s ======\n", config.title);
        CBLOG_INFO("  %d requests completed in %.2f seconds\n", config.requests_finished,
                (float)config.totlatency/1000);
        CBLOG_INFO("  %d clients threads\n", config.num_clients);
        CBLOG_INFO("  %d sdk threads\n", config.sdk_threads);
        CBLOG_INFO("  %d bytes payload\n", config.datasize);
        CBLOG_INFO("  %d pipeline\n", config.pipe_line);
        CBLOG_INFO("  keep alive: %d\n", config.keep_alive);
        CBLOG_INFO("\n");

        qsort(config.latency,config.requests,sizeof(long long),compareLatency);
        for (i = 0; i < config.requests; i++) {
            /*if (i%500 == 0) {
              printf("%lld...%lld\r\n", i, config.latency[i]);
              }*/
            if (config.latency[i]/1000 != curlat || i == (config.requests-1)) {
                curlat = config.latency[i]/1000;
                perc = ((float)(i+1)*100)/config.requests;
                //printf("%.2f%% <= %lld milliseconds\n", perc, curlat);
                CBLOG_INFO("%.2f%% <= %lld milliseconds\n", perc, curlat);
            }
        }
        //printf("%.2f requests per second\n\n", reqpersec);
        CBLOG_INFO("%.2f requests per second\n\n", reqpersec);
    } else if (config.csv) {
        //printf("\"%s\",\"%.2f\"\n", config.title, reqpersec);
        CBLOG_INFO("\"%s\",\"%.2f\"\n", config.title, reqpersec);
    } else {
        //printf("%s: %.2f requests per second\n", config.title, reqpersec);
        CBLOG_INFO("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}

/*
static int jimkv_int_count_bytes(uint64_t v) {
    int result = 1;
    while (1) {
        if (v < 10) {
            return result;
        }
        if (v < 100) {
            return result + 1;
        }
        if (v < 1000) {
            return result + 2;
        }
        if (v < 10000) {
            return result + 3;
        }
        v /= 10000U;
        result += 4;
    }
}
*/

static void bench(jimkv_benchmark_context_t *ctx) {
    int idx = 0;
    int argv_idx = 0;
    client c;
    jimkv_reply_t   *reply;
    size_t reply_num;
    jimkv_reply_t **pipe_reply = NULL;
    int pipe_size = ctx->cfg->pipe_line;

    //c = create_client_local();
    //jimkv_exec_cmd_t *cmd = jimkv_exec_cmd_create(&config, ctx->cmd_line);
    jimkv_exec_cmd_t *cmd = ctx->cmd;
    //data_t *_data;

    while (1) {
        if ((idx = __sync_fetch_and_add(&config.requests_issued, 1)) > config.requests)
        {
            break;
        }

        if (pipe_size > 1 && pipe_size < 10000) {
            void *pipe_handle;
            int j = 0;
            user_data_t *user_data = malloc(sizeof(user_data));
            user_data->idx = idx;
            user_data->start_time = ustime();
            pipe_handle = jimkv_async_pipe_cmd_create(space_context, 1000, sdk_pipe_reply_callback, user_data);
            for (;j < pipe_size; j++) {
                if (ctx->cfg->randomkeys) {
                    argv_idx = j % ctx->cfg->randomkeys_keyspacelen;
                } else {
                    argv_idx = j % 1000;
                }
                //_data = ctx->data+argv_idx;

                if (0 != jimkv_async_pipe_cmd_append(pipe_handle, cmd->fields,
                            (const char **)cmd->argv[argv_idx], cmd->argv_len))
                {
                    CBLOG_INFO("jimkv_pipe_command_append fail.");
                    break;
                }
            }
            jimkv_async_pipe_cmd_exec(pipe_handle);
            //c->latency = ustime()-(c->start);
        } else {
            if (ctx->cfg->randomkeys) {
                argv_idx = random() % ctx->cfg->randomkeys_keyspacelen;
            } else {
                argv_idx = random() % 1000;
            }
            //c->start = ustime();
            user_data_t *user_data = malloc(sizeof(user_data_t));
            user_data->idx = idx;
            user_data->start_time = ustime();
            jimkv_async_cmd_exec(space_context, cmd->fields,
                    (const char **)cmd->argv[argv_idx], cmd->argv_len, 1000, sdk_reply_callback, user_data);
            //c->latency = ustime()-(c->start);

            //ignore reply result
            //CBLOG_INFO("reply type: %d", reply->type);
            //jimkv_reply_free(reply);
        }


        //config.latency[idx] = c->latency;
    }
    //jimkv_exec_cmd_destroy(cmd);
    //free_client(c);
}

//static void benchmark(jimkv_exec_cmd_t *cmd, const int pipe_size) {
//    int i, idx;
//    char tmp[13];
//    client c;
//    jimkv_reply_t   *reply;
//    size_t reply_num;
//    jimkv_reply_t **pipe_reply = NULL;
//
//    c = create_client_local();
//
//    while (1) {
//        if ((idx = __sync_fetch_and_add(&config.requests_issued, 1))
//                > config.requests)
//        {
//            break;
//        }
//        for (i = 1; i < cmd->fields; i++) {
//            char *p = strstr(cmd->argv[i][0], "__rand_int__");
//            if (p != NULL) {
//                p[12] = '\0';
//                //CBLOG_INFO("key before: %s len: %zu", cmd->argv[i], cmd->argv_len[i]);
//                sprintf(tmp, "%d", idx%(1000));
//                strncat(cmd->argv[i], tmp, strlen(tmp));
//                cmd->argv[i][strlen(cmd->argv[i])] = '#';
//                //CBLOG_INFO("key after: %s len: %zu", cmd->argv[i], cmd->argv_len[i]);
//                break;
//            }
//        }
//
//        if (pipe_size > 1 && pipe_size < 10000) {
//            void *pipe_handle;
//            int j = 0;
//            c->start = ustime();
//            pipe_handle = jimkv_pipe_command_create(space_context, 0);
//            for (;j < pipe_size; j++) {
//                if (0 != jimkv_pipe_command_append(pipe_handle, cmd->fields,
//                            (const char **)cmd->argv, cmd->argv_len))
//                {
//                    CBLOG_INFO("jimkv_pipe_command_append fail.");
//                    break;
//                }
//            }
//            //jimkv_reply_t **jimkv_pipe_command_exec(size_t *reply_num, void *pipe_context);
//            pipe_reply = jimkv_pipe_command_exec(&reply_num, pipe_handle);
//            c->latency = ustime()-(c->start);
//
//            if (reply_num == 0 || pipe_reply == NULL) {
//                CBLOG_ERROR("jimkv_pipe_command_exec execute error.");
//                return;
//            }
//            jimkv_pipe_reply_free(pipe_reply, reply_num);
//
//        } else {
//            c->start = ustime();
//            reply = jimkv_command_exec(space_context, cmd->fields, (const char **)cmd->argv, cmd->argv_len, 1000);
//            c->latency = ustime()-(c->start);
//
//            //ignore reply result
//            jimkv_reply_free(reply);
//        }
//
//
//        if (__sync_bool_compare_and_swap(&config.requests_finished, config.requests,
//                config.requests))
//        {
//            break;
//        }
//        __sync_fetch_and_add(&config.requests_finished, 1);
//        config.latency[idx] = c->latency;
//    }
//    free_client(c);
//}

/* Returns number of consumed options. */
int parse_options(int argc, const char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.num_clients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-g")) {
            if (lastarg) goto invalid;
            config.conf_id = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-C")) {
            if (lastarg) goto invalid;
            config.sdk_threads = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keep_alive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            config.space_id = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.cfs_url = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-a") ) {
            if (lastarg) goto invalid;
            config.auth = strdup(argv[++i]);
            config.token = config.auth;
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipe_line = atoi(argv[++i]);
            if (config.pipe_line <= 0) config.pipe_line=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[++i]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 1000;
        } else if (!strcasecmp(argv[i],"-v")) {
            config.vma = atoi(argv[++i]);
            if (config.vma != 1) {
                config.vma = 0;
            }
        } else if (!strcasecmp(argv[i],"-X")) {
            config.cache_flag = atoi(argv[++i]);
            if (config.cache_flag != 1) {
                config.cache_flag = 0;
            }
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idle_mode = 1;
        } else if (!strcmp(argv[i],"-T")) {
            if (lastarg) goto invalid;
            config.run_time = atoi(argv[++i]);
            config.run_time *= 1000;
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = calloc(1, 1024);
            strcat(config.tests, ",");
            strcat(config.tests,(char*)argv[++i]);
            strcat(config.tests,",");

            char *p = config.tests;
            for ( ; *p; ++p ) *p = tolower(*p);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = "1";
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
            "Usage: jimkv_sdk_benchmark [-h <space_id>] [-r <token>] [-p <cfs_url>] [-c <clients>] [-n <requests]> [-d <size>] [-t <set,get>] [-P <cmds>]\n\n"
            " -h <space_id>      Cluster space (default 4876)\n"
            " -a <token>         Cluster token for Auth (default 2575746723354295607)\n"
            " -p <cfs>           Cfs url (default cfs.jim.jd.local)\n"
            /*" -s <socket>        Server socket (not supported)\n"*/
            " -c <clients>       Number of parallel connections (default 50)\n"
            " -C <skd_threads>   Number of sdk core threads (default 1)\n"
            " -g <config_id>     Cluster config id (default 0)\n"
            " -n <requests>      Total number of requests (default 100000)\n"
            " -d <size>          Data size of SET/GET value in bytes (default 2)\n"
            " -P <cmds>          Pipeline size of commands(default 0, no pipeline)\n"
            " -t <tests>         Only run the comma separated list of tests. The test\n"
            "                    names are the same as the ones produced as output.\n"
            " -l                 Loop. Run the tests forever\n"
            " -T                 run time of exit\n"
            " -dbnum <db>        SELECT the specified db number (default 0)\n"
            " -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
            " -v <boolean>       1=vma network 0=tcp network (default 0)\n"
            " -q                 Quiet. Just show query/sec values\n"
            /*" --csv              Output in CSV format\n"
              " -I                 Idle mode. Just open N idle connections and wait.\n\n"*/
            "Examples:\n\n"
            " Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
            "   $ jimkv_sdk_benchmark\n\n"
            " Use 20 parallel clients, for a total of 100k requests on 4876 cluster:\n"
            "   $ jimkv_sdk_benchmark -h 4876 -a 2575746723354295607 -n 100000 -c 20\n\n"
            );
    exit(exit_status);
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1, name, l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests, buf) != NULL;
}

//static void randomize_key(char *_key, size_t _len) {
//    size_t i;
//    char *p = _key;
//
//    for (i = 0; i < _len; i++) {
//        size_t r = random() % config.randomkeys_keyspacelen;
//        if (!r) continue;
//        //*p = '0'+r%10;
//        *p = '0'+r;
//    }
//}

jimkv_exec_cmd_t *jimkv_exec_cmd_create(config_t *cfg, const char *format, ...) {
    char ori_cmd[CMD_MAX_LENGTH];
    jimkv_exec_cmd_t *cmd = NULL;
    char tmp[64];

    int rk_cnt = data_number;

    va_list list;
    va_start(list,format);
    vsnprintf(ori_cmd, CMD_MAX_LENGTH, format, list);
    va_end(list);
    int i = 0;
    int j = 0;
    for (i = 0; i < rk_cnt; i++) {
        char *cmd_line = strdup(ori_cmd);
        char **_argv;
        size_t *_argv_len;
        int fields;

        _argv = split(cmd_line, ' ', 10, &fields);
        if (_argv == NULL || fields < 1) return NULL;

        if (fields && cmd == NULL) {
            cmd = malloc(sizeof(*cmd));
            _argv_len = calloc(10, sizeof(size_t));
            cmd->argv = calloc(rk_cnt, sizeof(char **));

            cmd->fields = fields;
            cmd->cmds = rk_cnt;
            cmd->argv_len = _argv_len;
        }

        for (j = 0; j < fields; j++) {
            _argv_len[j] = strlen(_argv[j]);
/*
            char *p = strstr(_argv[j], "__rand_int__##########");
            if (p != NULL) {
                //size_t len = strlen("__rand_int__##########");
                //randomize_key(p, len);
                //if (p != NULL) {
                p[12] = '\0';
                sprintf(tmp, "%ld", random() % 999999999);
                strncat(_argv[j], tmp, _argv_len[j]);
                _argv[j][strlen(_argv[j])] = '#';
                key_obj_t key_obj = malloc(sizeof(struct _key_obj_t));
                if (key_obj == NULL) {
                    fprintf(stderr,"Could not malloc memory,exit...");
                    exit(1);
                }
                key_obj->key_str = strdup(_argv[j]);

                if (config.run_time > 0) {
                    jimkv_list_push_back(&config.clearkeylist, &key_obj->link);
                }
            }
*/
        }
        cmd->argv[i] = _argv;
    }

    return cmd;
}

void jimkv_exec_cmd_destroy(jimkv_exec_cmd_t *cmd) {
    if (cmd == NULL) return;
    int i = 0;
    for (i = 0; i < cmd->cmds; i++) {
        free(cmd->argv[i][0]);
        free(cmd->argv[i]);
    }
    free(cmd->argv_len);
    free(cmd->argv);
    free(cmd);
}

//static jimkv_benchmark_context_t *jimkv_benchmark_context_create(jimkv_exec_cmd_t *cmd) {
//    jimkv_benchmark_context_t *ctx;
//    ctx = malloc(sizeof(*ctx));
//    if (ctx == NULL) {
//        CBLOG_ERROR("OOM");
//        return NULL;
//    }
//
//    ctx->cfg = &config;
//    ctx->cmd = cmd;
//    return ctx;
//}

static jimkv_benchmark_context_t *jimkv_bench_ctx_create(const char *cmd) {
    jimkv_benchmark_context_t *ctx;
    ctx = malloc(sizeof(*ctx));
    if (ctx == NULL) {
        CBLOG_ERROR("OOM");
        return NULL;
    }
    ctx->cmd = NULL;

    ctx->cfg = &config;
    ctx->cmd_line = malloc(CMD_MAX_LENGTH);
    strncpy(ctx->cmd_line, cmd, CMD_MAX_LENGTH);

    return ctx;
}

static void jimkv_context_destroy(jimkv_benchmark_context_t *ctx) {
    if (ctx != NULL) {
        //        jimkv_exec_cmd_destroy(ctx->cmd);
        ctx->cmd = NULL;

        if (ctx->cmd_line != NULL) {
            free(ctx->cmd_line);
        }
        free(ctx);
    }
}

static void *func_cmd_consume(void *ctx_data) {
    jimkv_benchmark_context_t *ctx = ctx_data;
    bench(ctx);
    jimkv_context_destroy(ctx);
    return NULL;
}

void jimkv_config_free(config_t *config) {
    if (config != NULL) {
        if (config->tests != NULL) {
            free(config->tests);
        }
        if (config->latency != NULL) {
            free(config->latency);
        }
    }
}

void *main_benchmark(void *param) {
    char *data;
    pthread_t tid[128];
    data_number = config.randomkeys_keyspacelen;
    if (data_number < 1)  {
        data_number = SDK_RAND_KV_DEFAULT_SIZE;
    }
    //if (data_number < config.pipe_line && config.pipe_line > 1) {
    //    data_number = config.pipe_line;
    //}

    data_number = SDK_RAND_KV_DEFAULT_SIZE;
    data_size = config.datasize;
    data_init();

    /* Run default benchmark suite. */
    data = malloc(config.datasize+1);
    do {
        memset(data,'x',config.datasize);
        data[config.datasize] = '\0';

        /*
           if (test_is_selected("ping_inline") || test_is_selected("ping")) {
           benchmark("PING_INLINE","PING\r\n");
           jimkv_exec_cmd_destroy(cmd);
           }

           if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
           cmd = jimkv_exec_cmd_create("PING");
           benchmark("PING_BULK",cmd);
           jimkv_exec_cmd_destroy(cmd);
           }*/

        if (test_is_selected("set")) {
            pthread_attr_t *attr = NULL;
            config.title = "SET";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "SET key:__rand_int__########## %s", data);
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("SET key:__rand_int__########## %s",data);
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);

            //show report
            config.totlatency = mstime()-config.start;

            showLatencyReport();
        }

        if (test_is_selected("get")) {
            pthread_attr_t *attr = NULL;
            config.title = "GET";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "GET key:__rand_int__##########");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("GET key:__rand_int__##########");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);

            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("incr")) {
            pthread_attr_t *attr = NULL;
            config.title = "INCR";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "INCR counter:__rand_int__##########");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("INCR counter:__rand_int__##########");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lpush")) {
            pthread_attr_t *attr = NULL;
            config.title = "LPUSH";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LPUSH mylist %s", data);
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LPUSH mylist %s", data);
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lpop")) {
            pthread_attr_t *attr = NULL;
            config.title = "LPOP";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LPOP mylist");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LPOP mylist");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("sadd")) {
            pthread_attr_t *attr = NULL;
            config.title = "SADD";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "SADD myset elemet:__rand_int__##########");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("SADD myset elemet:__rand_int__##########");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("spop")) {
            pthread_attr_t *attr = NULL;
            config.title = "SPOP";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "SPOP myset");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("SPOP myset");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lrange") ||
                test_is_selected("lrange_100") ||
                test_is_selected("lrange_300") ||
                test_is_selected("lrange_500") ||
                test_is_selected("lrange_600"))
        {
            pthread_attr_t *attr = NULL;
            config.title = "LPUSH (needed to benchmark LRANGE)";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LPUSH mylist %s", data);
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LPUSH mylist %s", data);
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;
                assert(ctx->cmd != NULL);

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")){
            pthread_attr_t *attr = NULL;
            config.title = "LRANGE_100 (first 100 elements)";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LRANGE mylist 0 99");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LRANGE mylist 0 99");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;
                assert(ctx->cmd != NULL);

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")){
            pthread_attr_t *attr = NULL;
            config.title = "LRANGE_100 (first 300 elements)";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LRANGE mylist 0 299");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LRANGE mylist 0 299");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;
                assert(ctx->cmd != NULL);

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")){
            pthread_attr_t *attr = NULL;
            config.title = "LRANGE_100 (first 500 elements)";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LRANGE mylist 0 499");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LRANGE mylist 0 499");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;
                assert(ctx->cmd != NULL);

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")){
            pthread_attr_t *attr = NULL;
            config.title = "LRANGE_100 (first 600 elements)";
            config.start = mstime();
            config.requests_issued = 0;
            config.requests_finished = 0;

            int idx;
            char cmd_line[CMD_MAX_LENGTH];
            snprintf(cmd_line, CMD_MAX_LENGTH, "LRANGE mylist 0 599");
            jimkv_exec_cmd_t *_cmd = jimkv_exec_cmd_create(&config, cmd_line);

            for (idx=0; idx<config.num_clients; idx++) {
                //cmd = jimkv_exec_cmd_create("LRANGE mylist 0 599");
                //jimkv_benchmark_context_t *ctx = jimkv_benchmark_context_create(cmd);
                jimkv_benchmark_context_t *ctx = jimkv_bench_ctx_create(cmd_line);
                ctx->cmd = _cmd;
                assert(ctx->cmd != NULL);

                pthread_create(&tid[idx], attr, func_cmd_consume, (void *)ctx);
            }

            for (idx=0; idx<config.num_clients; idx++) {
                pthread_join(tid[idx], NULL);
            }
            jimkv_exec_cmd_destroy(_cmd);
            //show report
            config.totlatency = mstime()-config.start;
            showLatencyReport();
        }

        /* not support temporarily
           if (test_is_selected("mset")) {
           const char *argv[21];
           argv[0] = "MSET";
           for (i = 1; i < 21; i += 2) {
           argv[i] = "key:__rand_int__##########";
           argv[i+1] = data;
           }
        //not support
        cmd = jimkv_exec_cmd_create("%s",argv);
        benchmark("MSET (10 keys)",cmd);
        jimkv_exec_cmd_destroy(cmd);
        }
        */
/*
        if (config.run_time > 0) {
            key_obj_t entry = NULL;
            key_obj_t hold = NULL;
            list_for_each_entry_safe(&config.clearkeylist, entry, hold, link) {
                //CBLOG_INFO("del key: %s", entry->key_str);
                exec_cmd_del(entry->key_str);
                jimkv_list_erase(&config.clearkeylist, &entry->link);
            }
            exec_cmd_del("mylist");
            exec_cmd_del("myset");
        }
*/
        if (!config.csv) printf("\n");
    } while(config.loop);

    if (data != NULL) {
        free(data);
    }
    return NULL;
}

void timeout_process() {
    CBLOG_INFO("timeout_process start");
    CBLOG_INFO("config.tests: %s", config.tests);
    memset(config.tests,'x',strlen(config.tests));
    config.loop = 0;
    __sync_fetch_and_add(&config.requests_issued, config.requests + 1);
    //config.requests_finished = config.requests;
    //__sync_fetch_and_add(&config.requests_finished, 1);
    CBLOG_INFO("timeout_process end");
}

int64_t exec_cmd_del(char *key) {
    char *cmd[2];
    size_t cmd_len[2];
    int64_t len = 0;
    int i = 0;
    cmd[i] = "del";
    cmd_len[i++] = 3;
    cmd[i] = key;
    cmd_len[i++] = strlen(key);
    jimkv_reply_t *rep = jimkv_command_exec(space_context,
            2, (const char **)cmd, cmd_len, 1000);
    if (rep != NULL) {
        len = rep->integer;
        jimkv_reply_free(rep);
    }
    return len;
}

int main(int argc, const char **argv) {
    int i;
    //char *data;
    //jimkv_exec_cmd_t *cmd;

    //pthread_t tid[128];

    srandom(time(NULL));
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.num_clients = 10;
    config.sdk_threads = 1;
    config.requests = 100000;
    config.live_clients = 0;
    config.keep_alive = 1;
    config.datasize = 3;
    config.pipe_line = 1;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 1000;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.run_time = -1;
    config.idle_mode = 0;
    config.latency = NULL;
    //jimkv_list_init(&config.clients);
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.auth = "2575746723354295607";
    config.space_id = "4876";
    config.token = config.auth;
    config.cfs_url = "cfs.jim.jd.local";
    config.conf_id = 0;
    config.vma = 0;

    i = parse_options(argc,argv);
    argc -= i;
    argv += i;
    pthread_mutex_init(&fin_mutex, NULL);
    pthread_cond_init(&fin_cond, NULL);
    //latency each request
    config.latency = malloc(sizeof(long long)*config.requests);

    char key_prefix[] = "key:,123,456";
    jimkv_space_option_t *opt = jimkv_space_option_create("11.3.90.194:443", 100, 1, 2, 8);

    jimkv_log_level_set(JIMKV_LOG_INFO);
    CBLOG_INFO("space_id: %s token:%s cfs:%s vma: %d", config.space_id,
            config.token, config.cfs_url, config.vma);

    if ((cc_handle = jimkv_client_create(0, config.sdk_threads)) == NULL) {
        CBLOG_ERROR( "init error");
        exit(1);
    }
    space_context = jimkv_client_add_space(cc_handle, opt);
    if (space_context == NULL) {
        CBLOG_ERROR("add space failed");
        exit(1);
    }
    int ret = jimkv_client_start(cc_handle);
    if (ret) {
        CBLOG_ERROR("client start failed");
        exit(1);
    }

    if (config.run_time <= 0) {
        main_benchmark(NULL);
        return 0;
    }

    exec_cmd_del("mylist");
    exec_cmd_del("myset");

    jimkv_config_free(&config);
    jimkv_client_destroy(cc_handle);
    return 0;
}
