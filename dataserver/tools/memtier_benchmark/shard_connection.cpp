/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "shard_connection.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "connections_manager.h"

shard_connection::shard_connection(unsigned int id, connections_manager* conns_man,
        benchmark_config* config, struct event_base* event_base,
        abstract_protocol* abs_protocol):
    connection(id, conns_man, config, event_base, abs_protocol)
{
    //m_db_selection = select_done;
    //m_authentication = auth_none;
    //m_cluster_slots = slots_done;
    m_pipeline = new std::queue<request *>;
    assert(m_pipeline != NULL);
}

shard_connection::~shard_connection() {
    if (m_pipeline != NULL) {
        delete m_pipeline;
        m_pipeline = NULL;
    }
}

request* shard_connection::pop_req() {
    request* req = m_pipeline->front();
    m_pipeline->pop();

    m_pending_resp--;
    assert(m_pending_resp >= 0);

    return req;
}

void shard_connection::push_req(request* req) {
    m_pipeline->push(req);
    m_pending_resp++;
}

bool shard_connection::is_conn_setup_done() {
    return m_authentication == auth_done &&
           m_db_selection == select_done &&
           m_cluster_slots == slots_done;
}

void shard_connection::send_conn_setup_commands(struct timeval timestamp) {
    if (m_authentication == auth_none) {
        benchmark_debug_log("sending authentication command.\n");
        m_protocol->authenticate(m_config->authenticate);
        push_req(new request(rt_auth, 0, &timestamp, 0));
        m_authentication = auth_sent;
    }

    if (m_db_selection == select_none) {
        benchmark_debug_log("sending db selection command.\n");
        m_protocol->select_db(m_config->select_db);
        push_req(new request(rt_select_db, 0, &timestamp, 0));
        m_db_selection = select_sent;
    }

    if (m_cluster_slots == slots_none) {
        benchmark_debug_log("sending cluster slots command.\n");

        // in case we send CLUSTER SLOTS command, we need to keep the response to parse it
        m_protocol->set_keep_value(true);
        m_protocol->write_command_cluster_slots();
        push_req(new request(rt_cluster_slots, 0, &timestamp, 0));
        m_cluster_slots = slots_sent;
    }
}

void shard_connection::process_response(void)
{
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    while ((ret = m_protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        request* req = pop_req();

        if (req->m_type == rt_auth) {
            if (r->is_error()) {
                benchmark_error_log("error: authentication failed [%s]\n", r->get_status());
                error = true;
            } else {
                m_authentication = auth_done;
                benchmark_debug_log("authentication successful.\n");
            }
        } else if (req->m_type == rt_select_db) {
            if (strcmp(r->get_status(), "+OK") != 0) {
                benchmark_error_log("database selection failed.\n");
                error = true;
            } else {
                benchmark_debug_log("database selection successful.\n");
                m_db_selection = select_done;
            }
        } else if (req->m_type == rt_cluster_slots) {
            if (r->get_mbulk_value() == NULL || r->get_mbulk_value()->mbulks_elements.size() == 0) {
                benchmark_error_log("cluster slot failed.\n");
                error = true;
            } else {
                // parse response
                m_conns_manager->handle_cluster_slots(r);
                m_protocol->set_keep_value(false);

                m_cluster_slots = slots_done;
                benchmark_debug_log("cluster slot command successful\n");
            }
        } else {
            benchmark_debug_log("server %s: handled response (first line): %s, %d hits, %d misses\n",
                                get_readable_id(),
                                r->get_status(),
                                r->get_hits(),
                                req->m_keys - r->get_hits());

            m_conns_manager->handle_response(m_id, now, req, r);
            m_conns_manager->inc_reqs_processed();
            responses_handled = true;
        }
        delete req;
        if (error) {
            return;
        }
    }

    if (ret == -1) {
        benchmark_error_log("error: response parsing failed.\n");
    }

    if (m_config->reconnect_interval > 0 && responses_handled) {
        if ((m_conns_manager->get_reqs_processed() % m_config->reconnect_interval) == 0) {
            assert(m_pipeline->size() == 0);
            benchmark_debug_log("reconnecting, m_reqs_processed = %u\n", m_conns_manager->get_reqs_processed());

            // client manage connection & disconnection of shard
            m_conns_manager->disconnect();
            ret = m_conns_manager->connect();
            assert(ret == 0);

            return;
        }
    }

    fill_pipeline();
}

void shard_connection::process_first_request() {
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void shard_connection::fill_pipeline(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);

    while (!m_conns_manager->finished() && m_pipeline->size() < m_config->pipeline) {
        if (!is_conn_setup_done()) {
            send_conn_setup_commands(now);
            return;
        }

        // don't exceed requests
        if (m_conns_manager->hold_pipeline(m_id))
            break;

        // client manage requests logic
        m_conns_manager->create_request(now, m_id);
    }
}

void shard_connection::handle_event(short evtype)
{
    connection::handle_event(evtype);
}

void shard_connection::send_wait_command(struct timeval* sent_time,
                                         unsigned int num_slaves, unsigned int timeout) {
    int cmd_size = 0;

    benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);

    cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
    push_req(new request(rt_wait, cmd_size, sent_time, 0));
}

void shard_connection::send_set_command(struct timeval* sent_time, const char *key, int key_len,
                                        const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("server %s: SET key=[%.*s] value_len=%u expiry=%u\n",
                        get_readable_id(), key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
                                             expiry, offset);

    push_req(new request(rt_set, cmd_size, sent_time, 1));
}

void shard_connection::send_get_command(struct timeval* sent_time,
                                        const char *key, int key_len, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("server %s: GET key=[%.*s]\n", get_readable_id(), key_len, key);
    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(new request(rt_get, cmd_size, sent_time, 1));
}

void shard_connection::send_mget_command(struct timeval* sent_time, const keylist* key_list) {
    int cmd_size = 0;

    const char *first_key, *last_key;
    unsigned int first_key_len, last_key_len;
    first_key = key_list->get_key(0, &first_key_len);
    last_key = key_list->get_key(key_list->get_keys_count()-1, &last_key_len);

    benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n",
                        key_list->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

    cmd_size = m_protocol->write_command_multi_get(key_list);

    push_req(new request(rt_get, cmd_size, sent_time, key_list->get_keys_count()));
}

void shard_connection::send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
                                               const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    benchmark_debug_log("GET key=[%.*s] value_len=%u expiry=%u\n",
                        key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(new verify_request(rt_get, cmd_size, sent_time, 1, key, key_len, value, value_len));
}

/*
 * arbitrary command:
 *
 * we send the arbitrary command in several iterations, where on each iteration
 * different type of argument can be sent (const/randomized).
 *
 * since we do it on several iterations, we call to arbitrary_command_end() to mark that
 * all the command sent
 */

int shard_connection::send_arbitrary_command(const command_arg *arg) {
    int cmd_size = 0;

    cmd_size = m_protocol->write_arbitrary_command(arg);

    return cmd_size;
}

int shard_connection::send_arbitrary_command(const command_arg *arg, const char *val, int val_len) {
    int cmd_size = 0;

    if (arg->type == key_type) {
        benchmark_debug_log("key: value[%.*s]\n",  val_len, val);
    } else {
        benchmark_debug_log("data: value_len=%u\n",  val_len);
    }

    cmd_size = m_protocol->write_arbitrary_command(val, val_len);

    return cmd_size;
}

void shard_connection::send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size) {
    push_req(new arbitrary_request(command_index, rt_arbitrary, cmd_size, sent_time));
}
