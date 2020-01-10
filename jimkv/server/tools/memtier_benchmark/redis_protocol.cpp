#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "redis_protocol.h"

int redis_protocol::select_db(int db)
{
    int size = 0;
    char db_str[20];

    snprintf(db_str, sizeof(db_str)-1, "%d", db);
    size = evbuffer_add_printf(m_write_buf,
        "*2\r\n"
        "$6\r\n"
        "SELECT\r\n"
        "$%u\r\n"
        "%s\r\n",
        (unsigned int)strlen(db_str), db_str);
    return size;
}

int redis_protocol::authenticate(const char *credentials)
{
    int size = 0;
    assert(credentials != NULL);

    size = evbuffer_add_printf(m_write_buf,
        "*2\r\n"
        "$4\r\n"
        "AUTH\r\n"
        "$%u\r\n"
        "%s\r\n",
        (unsigned int)strlen(credentials), credentials);
    return size;
}

int redis_protocol::write_command_cluster_slots()
{
    int size = 0;

    size = evbuffer_add(m_write_buf,
                        "*2\r\n"
                        "$7\r\n"
                        "CLUSTER\r\n"
                        "$5\r\n"
                        "SLOTS\r\n",
                        28);

    return size;
}

int redis_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    if (!expiry && !offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*3\r\n"
            "$3\r\n"
            "SET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n", value_len);
    } else if(offset) {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "SETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(offset_str), offset_str, value_len);
    } else {
        char expiry_str[30];
        snprintf(expiry_str, sizeof(expiry_str)-1, "%u", expiry);
        
        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$5\r\n"
            "SETEX\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(expiry_str), expiry_str, value_len);
    }
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int redis_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for redis yet!\n");
    assert(0);
}

int redis_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;
    
    if (!offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*2\r\n"
            "$3\r\n"
            "GET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        evbuffer_add(m_write_buf, "\r\n", 2);        
        size += key_len + 2;
    } else {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "GETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$2\r\n"
            "-1\r\n", (unsigned int) strlen(offset_str), offset_str);        
    }

    return size;
}

/*
 * Utility function to get the number of digits in a number
 */
static int get_number_length(unsigned int num)
{
    if (num < 10) return 1;
    if (num < 100) return 2;
    if (num < 1000) return 3;
    if (num < 10000) return 4;
    if (num < 100000) return 5;
    if (num < 1000000) return 6;
    if (num < 10000000) return 7;
    if (num < 100000000) return 8;
    if (num < 1000000000) return 9;
    return 10;
}

int redis_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    int size = 0;
    size = evbuffer_add_printf(m_write_buf,
                               "*3\r\n"
                               "$4\r\n"
                               "WAIT\r\n"
                               "$%u\r\n"
                               "%u\r\n"
                               "$%u\r\n"
                               "%u\r\n",
                               get_number_length(num_slaves), num_slaves,
                               get_number_length(timeout), timeout);
    return size;
}

int redis_protocol::parse_response(void)
{
    char *line;
    size_t res_len;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                // clear last response
                m_last_response.clear();
                m_response_len = 0;
                m_total_bulks_count = 0;
                m_response_state = rs_read_line;

                break;
            case rs_read_line:
                line = evbuffer_readln(m_read_buf, &res_len, EVBUFFER_EOL_CRLF_STRICT);

                // maybe we didn't get it yet?
                if (line == NULL) {
                    return 0;
                }

                // count CRLF
                m_response_len += res_len + 2;

                if (line[0] == '*') {
                    int count = strtol(line + 1, NULL, 10);

                    // in case of nested mbulk, the mbulk is one of the total bulks
                    if (m_total_bulks_count > 0) {
                        m_total_bulks_count--;
                    }

                    // from bulks counter perspective every count < 0 is equal to 0, because it's not followed by bulks.
                    if (count < 0) {
                        count = 0;
                    }

                    if (m_keep_value) {
                        mbulk_size_el* new_mbulk_size = new mbulk_size_el();
                        new_mbulk_size->bulks_count = count;
                        new_mbulk_size->upper_level = m_current_mbulk;

                        // update first mbulk as the response mbulk, or insert it to current mbulk
                        if (m_last_response.get_mbulk_value() == NULL) {
                            m_last_response.set_mbulk_value(new_mbulk_size);
                        } else {
                            m_current_mbulk->add_new_element(new_mbulk_size);
                        }

                        // update current mbulk
                        m_current_mbulk = new_mbulk_size->get_next_mbulk();
                    }

                    m_last_response.set_status(line);
                    m_total_bulks_count += count;

                    if (m_total_bulks_count == 0) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else if (line[0] == '$') {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    int len = strtol(line + 1, NULL, 10);
                    m_last_response.set_status(line);

                    if (len <= 0) {
                        m_bulk_len = 0;
                        m_response_state = rs_end_bulk;
                    } else {
                        m_bulk_len = (unsigned int) len;
                        m_response_state = rs_read_bulk;
                    }
                } else if (line[0] == '+' || line[0] == '-' || line[0] == ':') {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    // if we are not inside mbulk, the status will be kept in m_status anyway
                    if (m_keep_value && m_current_mbulk) {
                        char *bulk_value = strdup(line);
                        assert(bulk_value != NULL);

                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        new_bulk->value_len = strlen(bulk_value);

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    }

                    if (line[0] == '-')
                        m_last_response.set_error(true);

                    m_last_response.set_status(line);
                    m_total_bulks_count--;

                    if (m_total_bulks_count == 0) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else {
                    benchmark_debug_log("unsupported response: '%s'.\n", line);
                    free(line);
                    return -1;
                }
                break;
            case rs_read_bulk:
                if (evbuffer_get_length(m_read_buf) >= m_bulk_len + 2) {
                    m_response_len += m_bulk_len + 2;
                    m_last_response.incr_hits();

                    m_response_state = rs_end_bulk;
                } else {
                    return 0;
                }
                break;
            case rs_end_bulk:
                if (m_keep_value) {
                    /*
                     * keep bulk value - in case we need to save bulk value it depends
                     * if it's inside a mbulk or not.
                     * in case of receiving just bulk as a response, we save it directly to the m_last_response,
                     * otherwise we insert it to the current mbulk element
                     */
                    char *bulk_value = NULL;
                    if (m_bulk_len > 0) {
                        bulk_value = (char *) malloc(m_bulk_len);
                        assert(bulk_value != NULL);

                        int ret = evbuffer_remove(m_read_buf, bulk_value, m_bulk_len);
                        assert(ret != -1);

                        // drain CRLF
                        ret = evbuffer_drain(m_read_buf, 2);
                        assert(ret != -1);
                    }

                    // in case we are inside mbulk
                    if (m_current_mbulk) {
                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        new_bulk->value_len = m_bulk_len;

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    } else {
                        m_last_response.set_value(bulk_value, m_bulk_len);
                    }
                } else {
                    // just drain the buffer, include the CRLF
                    if (m_bulk_len > 0) {
                        int ret = evbuffer_drain(m_read_buf, m_bulk_len + 2);
                        assert(ret != -1);
                    }
                }

                m_total_bulks_count--;

                if (m_total_bulks_count == 0) {
                    m_last_response.set_total_len(m_response_len);
                    m_response_state = rs_initial;
                    return 1;
                } else {
                    m_response_state = rs_read_line;
                }
                break;
            default:
                return -1;
        }
    }

    return -1;
}

int redis_protocol::write_arbitrary_command(const command_arg *arg) {
    evbuffer_add(m_write_buf, arg->data.c_str(), arg->data.length());

    return arg->data.length();
}

int redis_protocol::write_arbitrary_command(const char *rand_val, int rand_val_len) {
    int size = 0;

    size = evbuffer_add_printf(m_write_buf, "$%d\r\n", rand_val_len);
    evbuffer_add(m_write_buf, rand_val, rand_val_len);
    size += rand_val_len;
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += 2;

    return size;
}

bool redis_protocol::format_arbitrary_command(arbitrary_command &cmd) {
    for (unsigned int i = 0; i < cmd.command_args.size(); i++) {
        command_arg* current_arg = &cmd.command_args[i];
        current_arg->type = const_type;

        // check arg type
        if (current_arg->data.find(KEY_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(KEY_PLACEHOLDER)) {
                benchmark_error_log("error: key placeholder can't combined with other data\n");
                return false;
            }

            current_arg->type = key_type;
        } else if (current_arg->data.find(DATA_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(DATA_PLACEHOLDER)) {
                benchmark_error_log("error: data placeholder can't combined with other data\n");
                return false;
            }

            current_arg->type = data_type;
        }

        // we expect that first arg is the COMMAND name
        assert(i != 0 || (i == 0 && current_arg->type == const_type && "first arg is not command name?"));

        if (current_arg->type == const_type) {
            char buffer[20];
            int buffer_len;

            // if it's first arg we add also the mbulk size
            if (i == 0) {
                buffer_len = snprintf(buffer, 20, "*%zd\r\n$%zd\r\n", cmd.command_args.size(), current_arg->data.length());
            } else {
                buffer_len = snprintf(buffer, 20, "$%zd\r\n", current_arg->data.length());
            }

            current_arg->data.insert(0, buffer, buffer_len);
            current_arg->data += "\r\n";
        }
    }

    return true;
}
