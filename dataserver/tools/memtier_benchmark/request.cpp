#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include "request.h"
request::request(request_type type, unsigned int size, struct timeval* sent_time, unsigned int keys)
        : m_type(type), m_size(size), m_keys(keys)
{
    if (sent_time != NULL)
        m_sent_time = *sent_time;
    else {
        gettimeofday(&m_sent_time, NULL);
    }
}

arbitrary_request::arbitrary_request(size_t request_index, request_type type,
                                     unsigned int size, struct timeval* sent_time) :
    request(type, size, sent_time, 1),
    index(request_index)
{

}

verify_request::verify_request(request_type type,
                               unsigned int size,
                               struct timeval* sent_time,
                               unsigned int keys,
                               const char *key,
                               unsigned int key_len,
                               const char *value,
                               unsigned int value_len) :
        request(type, size, sent_time, keys),
        m_key(NULL), m_key_len(0),
        m_value(NULL), m_value_len(0)
{
    m_key_len = key_len;
    m_key = (char *)malloc(key_len);
    memcpy(m_key, key, m_key_len);

    m_value_len = value_len;
    m_value = (char *)malloc(value_len);
    memcpy(m_value, value, m_value_len);
}

verify_request::~verify_request(void)
{
    if (m_key != NULL) {
        free((void *) m_key);
        m_key = NULL;
    }
    if (m_value != NULL) {
        free((void *) m_value);
        m_value = NULL;
    }
}
