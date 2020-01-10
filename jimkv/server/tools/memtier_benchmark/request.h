#ifndef MEMTIER_BENCHMARK_REQUEST_H
#define MEMTIER_BENCHMARK_REQUEST_H
enum request_type { rt_unknown, rt_set, rt_get, rt_wait, rt_arbitrary, rt_auth, rt_select_db, rt_cluster_slots, rt_heartbeat };
struct request {
    request_type m_type;
    struct timeval m_sent_time;
    unsigned int m_size;
    unsigned int m_keys;

    request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys);
    virtual ~request(void) {}
};

struct arbitrary_request : public request {
    size_t index;

    arbitrary_request(size_t request_index, request_type type,
                      unsigned int size, struct timeval* sent_time);
    virtual ~arbitrary_request(void) {}
};

struct verify_request : public request {
    char *m_key;
    unsigned int m_key_len;
    char *m_value;
    unsigned int m_value_len;

    verify_request(request_type type,
                   unsigned int size,
                   struct timeval* sent_time,
                   unsigned int keys,
                   const char *key,
                   unsigned int key_len,
                   const char *value,
                   unsigned int value_len);
    virtual ~verify_request(void);
};
#endif //MEMTIER_BENCHMARK_REQUEST_H
