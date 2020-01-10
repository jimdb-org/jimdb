#include "batch_stress.h"

#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include <sys/syscall.h>

#include "jimkv_client.h"
#include "jimkv_log.h"

void *cc = NULL;
void *space_context = NULL;
size_t totle = 0,right_all = 0, wrong_all = 0;

uint64_t g_req_number = 0;
uint64_t g_req_time = 0; //unit is seconds
uint64_t g_seq = 0;
bs_type_call_t bs_type_call[BS_COMMAND_TYPE_COUNT];

void set_command_type() {
    bs_type_call[0]          = string_command;
    //bs_type_call[BS_COMMAND_KEYS]            = keys_command;
    //bs_type_call[BS_COMMAND_STRING]          = string_command;
    //bs_type_call[BS_COMMAND_HASH]            = hash_command;
    //bs_type_call[BS_COMMAND_LIST]            = list_command;
    //bs_type_call[BS_COMMAND_SET]             = set_command;
    //bs_type_call[BS_COMMAND_SORTED_SET]      = sorted_set_command;
    //bs_type_call[BS_COMMAND_HYPER_LOG_LOG]   = hyper_log_log_command;
}
static pid_t get_tid() {
    return syscall(__NR_gettid);
}
void *thread_write(void *arg) {
    char *key = NULL;
    int key_len = 0;
    int hash_code;
    int i = 0;
    unsigned int seed = time(NULL);

    bs_type_call_t type_call;

    while (1) {
        if (i > 10000) {
            seed = time(NULL) + get_tid();
            i = 0;
        }
        key_len = rand_r(&seed) % BS_KEY_MAX_LEN + 8;
        if (key_len <= 8) {
            key_len = BS_STR_DEFAULT_LEN;
        }
        key = gen_rand_string(&key_len, &seed, BS_KEY);
        g_seq = __sync_add_and_fetch(&g_seq, 1);
        snprintf(key + key_len-9, 8, "%08lu", g_seq);
        hash_code = mur_hash(key, key_len);
        type_call = bs_type_call[hash_code % BS_COMMAND_TYPE_COUNT];
        type_call(key, &seed, hash_code, BS_WRITE_COMMAND);
        i++;
    }
    return NULL;
}

void *thread_read(void *arg) {
    char *key = NULL;
    int i = 0;
    int key_len = 0;
    int hash_code;

    unsigned int seed = time(NULL);

    bs_type_call_t type_call;

    while (1) {
        if (i > 10000) {
            seed = time(NULL);
            i = 0;
        }
        key_len = rand_r(&seed) % BS_KEY_MAX_LEN;
        key = gen_rand_string(&key_len, &seed, BS_KEY);
        hash_code = mur_hash(key, key_len);

        type_call = bs_type_call[hash_code % BS_COMMAND_TYPE_COUNT];
        type_call(key, &seed, hash_code, BS_READ_COMMAND);
        i++;
    }
    return NULL;
}

void usage(char **argv) {
    fprintf(stderr, "%s write_threads read_threads cluster_id db_id table_id master_host\n", argv[0]);
}

void cmd_init() {
    set_command_type();

    keys_init();
    string_init();
    batch_hash_init();
    list_init();
    set_init();
    //sorted_set_init();
    //hyper_log_log_init();
}

#if defined(DEBUG_FLAG)
static void sigDumpHandler(int sig)
{
    static bool bDumpFlag = false;
    char filename[256];

    if (bDumpFlag) {
        return;
    }

    bDumpFlag = true;

    snprintf(filename, sizeof(filename),
        "%s/logs/mc_dump.log", "/tmp");
    //manager_dump_global_vars_to_file(filename);

    bDumpFlag = false;
}
#endif

static void sigHupHandler(int sig)
{
    CBLOG_INFO("catch signal: %d", sig);
}

static void sigUsrHandler(int sig)
{
    CBLOG_INFO("catch signal: %d", sig);
}

int setup_signal_handler()
{
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);

    act.sa_handler = sigUsrHandler;
    if(sigaction(SIGUSR1, &act, NULL) < 0 ||
        sigaction(SIGUSR2, &act, NULL) < 0)
    {
        CBLOG_INFO("exit abnormally!");
        return errno;
    }

    act.sa_handler = sigHupHandler;
    if(sigaction(SIGHUP, &act, NULL) < 0) {
        CBLOG_INFO("exit abnormally!");
        return errno;
    }

    act.sa_handler = SIG_IGN;
    if(sigaction(SIGPIPE, &act, NULL) < 0) {
        CBLOG_INFO("exit abnormally!");
        return errno;
    }

#if defined(DEBUG_FLAG)
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);
    act.sa_handler = sigDumpHandler;
    if(sigaction(SIGUSR1, &act, NULL) < 0 ||
        sigaction(SIGUSR2, &act, NULL) < 0)
    {
        CBLOG_ERROR("exit abnormally!");
        return errno;
    }
#endif
    return 0;
}

int main(int argc, char **argv) {
    int  vma = 0;
    uint64_t req_number = 0;
    uint64_t req_times = 0;
    uint64_t exec_req_number = 0;
    uint64_t exec_times = 0;

    pthread_attr_t attr;
    char log_level[] = {'i','n','f','o'};
    //char log_level[] = {'d','e','b','u', 'g'};
    long i;
    int res;
    pthread_t *wtid, *rtid;
    //int logfd = -1;

    if (argc != 7) {
        usage(argv);
        return -1;
    }
    char *endptr;

    int write_threads = atoi(argv[1]);
    int read_threads = atoi(argv[2]);

    uint64_t cluster_id = strtoul(argv[3], &endptr, 10);
    uint64_t db_id = strtoul(argv[4], &endptr, 10);
    uint64_t table_id = strtoul(argv[5], &endptr, 10);
    char *master_host =  strdup(argv[6]);

    setup_signal_handler();

    jimkv_log_level_set(JIMKV_LOG_WARN);
    jimkv_space_option_t *opt = jimkv_space_option_create(master_host, cluster_id, db_id, table_id, 8);

    cc = jimkv_client_create(0, 10);
    if (cc == NULL) {
        CBLOG_ERROR("init error");
        exit(1);
    }
    jimkv_client_start(cc);

    space_context = jimkv_client_add_space(cc, opt);
    if (!space_context) {
        CBLOG_ERROR("create space context failed");
        exit(1);
    }

    srand((unsigned)time(NULL));

    cmd_init();

    wtid = malloc(sizeof(pthread_t) * write_threads);
    rtid = malloc(sizeof(pthread_t) * read_threads);

    res = pthread_attr_init(&attr);
    if (res != 0) {
        CBLOG_ERROR("pthread_attr_init failed, error: %s", strerror(errno));
        exit(1);
    }

    res = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (res != 0) {
        CBLOG_ERROR("pthread_attr_setdetachstate failed, error: %s", strerror(errno));
        exit(1);
    }

    for (i=0; i<write_threads; i++) {
        if (pthread_create(&wtid[i], &attr, thread_write, (void *)i) != 0) {
            CBLOG_ERROR("write_threads %ld create fail!", i);
        }
    }

    for (i=0; i<read_threads; i++) {
        if (pthread_create(&rtid[i], &attr, thread_read, (void *)i) != 0) {
            CBLOG_ERROR("read_threads %ld create fail!", i);
        }
    }

    while (1) {
        sleep(5);
        exec_req_number = g_req_number - req_number;
        exec_times = g_req_time - req_times;
        CBLOG_INFO("exec command number: %"PRIu64", avg time: %"PRIu64, exec_req_number, exec_times/exec_req_number);
        req_number = g_req_number;
        req_times = g_req_time;
    }
    free(wtid);
    free(rtid);
    free(master_host);
    jimkv_client_destroy(cc);
    CBLOG_INFO("+++++++++++++++++end+++++++++++++++");

    return 0;
}
