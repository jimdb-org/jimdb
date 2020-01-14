#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif
#include "memcached_protocol.h"

int memcache_text_protocol::select_db(int db)
{
    assert(0);
}

int memcache_text_protocol::authenticate(const char *credentials)
{
    assert(0);
}

int memcache_text_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_text_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "set %.*s 0 %u %u\r\n", key_len, key, expiry, value_len);
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int memcache_text_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "get %.*s\r\n", key_len, key);
    return size;
}

int memcache_text_protocol::write_command_multi_get(const keylist *keylist)
{
    assert(keylist != NULL);
    assert(keylist->get_keys_count() > 0);

    int n = 0;
    int size = 0;

    n = evbuffer_add(m_write_buf, "get", 3);
    assert(n != -1);
    size = 3;

    for (unsigned int i = 0; i < keylist->get_keys_count(); i++) {
        const char *key;
        unsigned int key_len;

        n = evbuffer_add(m_write_buf, " ", 1);
        assert(n != -1);
        size++;

        key = keylist->get_key(i, &key_len);
        assert(key != NULL);

        n = evbuffer_add(m_write_buf, key, key_len);
        assert(n != -1);
        size += key_len;
    }

    n = evbuffer_add(m_write_buf, "\r\n", 2);
    assert(n != -1);
    size += 2;

    return size;
}

int memcache_text_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_text_protocol::parse_response(void)
{
    char *line;
    size_t tmplen;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                m_last_response.clear();
                m_response_state = rs_read_section;
                m_response_len = 0;
                break;

            case rs_read_section:
                line = evbuffer_readln(m_read_buf, &tmplen, EVBUFFER_EOL_CRLF_STRICT);
                if (!line)
                    return 0;

                m_response_len += tmplen + 2;   // For CRLF
                if (m_last_response.get_status() == NULL) {
                    m_last_response.set_status(line);
                }
                m_last_response.set_total_len((unsigned int) m_response_len);   // for now...

                if (memcmp(line, "VALUE", 5) == 0) {
                    char prefix[50];
                    char key[256];
                    unsigned int flags;
                    unsigned int cas;

                    int res = sscanf(line, "%s %s %u %u %u", prefix, key, &flags, &m_value_len, &cas);
                    if (res < 4|| res > 5) {
                        benchmark_debug_log("unexpected VALUE response: %s\n", line);
                        if (m_last_response.get_status() != line)
                            free(line);
                        return -1;
                    }

                    m_response_state = rs_read_value;
                    continue;
                } else if (memcmp(line, "END", 3) == 0 ||
                           memcmp(line, "STORED", 6) == 0) {
                    if (m_last_response.get_status() != line)
                        free(line);
                    m_response_state = rs_read_end;
                    break;
                } else {
                    m_last_response.set_error(true);
                    benchmark_debug_log("unknown response: %s\n", line);
                    return -1;
                }
                break;

            case rs_read_value:
                if (evbuffer_get_length(m_read_buf) >= m_value_len + 2) {
                    if (m_keep_value) {
                        char *value = (char *) malloc(m_value_len);
                        assert(value != NULL);

                        int ret = evbuffer_remove(m_read_buf, value, m_value_len);
                        assert((unsigned int) ret == 0);

                        m_last_response.set_value(value, m_value_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, m_value_len);
                        assert((unsigned int) ret == 0);
                    }

                    int ret = evbuffer_drain(m_read_buf, 2);
                    assert((unsigned int) ret == 0);

                    m_last_response.incr_hits();
                    m_response_len += m_value_len + 2;
                    m_response_state = rs_read_section;
                } else {
                    return 0;
                }
                break;
            case rs_read_end:
                m_response_state = rs_initial;
                return 1;

            default:
                benchmark_debug_log("unknown response state %d.\n", m_response_state);
                return -1;
        }
    }

    return -1;
}

bool memcache_text_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////


int memcache_binary_protocol::select_db(int db)
{
    assert(0);
}

int memcache_binary_protocol::authenticate(const char *credentials)
{
    protocol_binary_request_no_extras req;
    char nullbyte = '\0';
    const char mechanism[] = "PLAIN";
    int mechanism_len = sizeof(mechanism) - 1;
    const char *colon;
    const char *user;
    int user_len;
    const char *passwd;
    int passwd_len;

    assert(credentials != NULL);
    colon = strchr(credentials, ':');
    assert(colon != NULL);

    user = credentials;
    user_len = colon - user;
    passwd = colon + 1;
    passwd_len = strlen(passwd);

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SASL_AUTH;
    req.message.header.request.keylen = htons(mechanism_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(mechanism_len + user_len + passwd_len + 2);
    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, mechanism, mechanism_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, user, user_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, passwd, passwd_len);

    return sizeof(req) + user_len + passwd_len + 2 + sizeof(mechanism) - 1;
}

int memcache_binary_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_binary_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);

    protocol_binary_request_set req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(sizeof(req.message.body) + value_len + key_len);
    req.message.header.request.extlen = sizeof(req.message.body);
    req.message.body.expiration = htonl(expiry);

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);
    evbuffer_add(m_write_buf, value, value_len);

    return sizeof(req) + key_len + value_len;
}

int memcache_binary_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);

    protocol_binary_request_get req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(key_len);
    req.message.header.request.extlen = 0;

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);

    return sizeof(req) + key_len;
}

int memcache_binary_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for binary memcache yet!\n");
    assert(0);
}

const char* memcache_binary_protocol::status_text(void)
{
    int status;
    static const char* status_str_00[] = {
        "PROTOCOL_BINARY_RESPONSE_SUCCESS",
        "PROTOCOL_BINARY_RESPONSE_KEY_ENOENT",
        "PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS",
        "PROTOCOL_BINARY_RESPONSE_E2BIG",
        "PROTOCOL_BINARY_RESPONSE_EINVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_STORED",
        "PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET",
    };
    static const char* status_str_20[] = {
        "PROTOCOL_BINARY_RESPONSE_AUTH_ERROR",
        "PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE"
    };
    static const char* status_str_80[] = {
        NULL,
        "PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND",
        "PROTOCOL_BINARY_RESPONSE_ENOMEM",
        "PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED",
        "PROTOCOL_BINARY_RESPONSE_EINTERNAL",
        "PROTOCOL_BINARY_RESPONSE_EBUSY",
        "PROTOCOL_BINARY_RESPONSE_ETMPFAIL"
    };

    status = ntohs(m_response_hdr.message.header.response.status);
    if (status <= 0x07) {
        return status_str_00[status];
    } else if (status >= 0x20 && status <= 0x21) {
        return status_str_20[status - 0x20];
    } else if (status >= 0x80 && status <= 0x86) {
        return status_str_80[status - 0x80];
    } else {
        return NULL;
    }
}

int memcache_binary_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_binary_protocol::parse_response(void)
{
    while (true) {
        int ret;
        int status;

        switch (m_response_state) {
            case rs_initial:
                if (evbuffer_get_length(m_read_buf) < sizeof(m_response_hdr))
                    return 0;               // no header yet?

                ret = evbuffer_remove(m_read_buf, (void *)&m_response_hdr, sizeof(m_response_hdr));
                assert(ret == sizeof(m_response_hdr));

                if (m_response_hdr.message.header.response.magic != PROTOCOL_BINARY_RES) {
                    benchmark_error_log("error: invalid memcache response header magic.\n");
                    return -1;
                }

                m_response_len = sizeof(m_response_hdr);
                m_last_response.clear();
                if (status_text()) {
                    m_last_response.set_status(strdup(status_text()));
                }

                status = ntohs(m_response_hdr.message.header.response.status);
                if (status == PROTOCOL_BINARY_RESPONSE_AUTH_ERROR ||
                    status == PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_EBUSY) {
                    m_last_response.set_error(true);
                }

                if (ntohl(m_response_hdr.message.header.response.bodylen) > 0) {
                    m_response_hdr.message.header.response.bodylen = ntohl(m_response_hdr.message.header.response.bodylen);
                    m_response_hdr.message.header.response.keylen = ntohs(m_response_hdr.message.header.response.keylen);

                    m_response_state = rs_read_body;
                    continue;
                }

                return 1;
                break;
            case rs_read_body:
                if (evbuffer_get_length(m_read_buf) >= m_response_hdr.message.header.response.bodylen) {
                    // get rid of extras and key, we don't care about them
                    ret = evbuffer_drain(m_read_buf,
                        m_response_hdr.message.header.response.extlen +
                        m_response_hdr.message.header.response.keylen);
                    assert((unsigned int) ret == 0);

                    int actual_body_len = m_response_hdr.message.header.response.bodylen -
                        m_response_hdr.message.header.response.extlen -
                        m_response_hdr.message.header.response.keylen;
                    if (m_keep_value) {
                        char *value = (char *) malloc(actual_body_len);
                        assert(value != NULL);
                        ret = evbuffer_remove(m_read_buf, value, actual_body_len);
                        m_last_response.set_value(value, actual_body_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, actual_body_len);
                        assert((unsigned int) ret == 0);
                    }

                    if (m_response_hdr.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS)
                        m_last_response.incr_hits();

                    m_response_len += m_response_hdr.message.header.response.bodylen;
                    m_response_state = rs_initial;

                    return 1;
                } else {
                    return 0;
                }
                break;
            default:
                benchmark_debug_log("unknown response state.\n");
                return -1;
        }
    }

    return -1;
}

bool memcache_binary_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////
