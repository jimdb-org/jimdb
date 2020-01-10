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
#include <atomic>
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

#include "cbdb_connection.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "connections_manager.h"

std::atomic<std::uint64_t> global_seqence = {0};

cbdb_connection::cbdb_connection(unsigned int id, connections_manager* conns_man,
        benchmark_config* config, struct event_base* event_base,
        abstract_protocol* abs_protocol):
    connection(id, conns_man, config, event_base, abs_protocol)
{
    m_pipeline = new std::map<int, request *>;
    assert(m_pipeline != NULL);
}

cbdb_connection::~cbdb_connection() {
    if (m_pipeline != NULL) {
        delete m_pipeline;
        m_pipeline = NULL;
    }
}

request* cbdb_connection::pop_req(int msg_id) {
    request* req = (*m_pipeline)[msg_id];
    m_pipeline->erase(msg_id);

    m_pending_resp--;
    assert(m_pending_resp >= 0);

    return req;
}

void cbdb_connection::push_req(int msg_id, request* req) {
    m_pipeline->insert(std::pair<int, request*>(msg_id, req));
    m_pending_resp++;
}

bool cbdb_connection::is_conn_setup_done() {
    return true;
}

void cbdb_connection::send_conn_setup_commands(struct timeval timestamp) {
    benchmark_debug_log("call send_conn_setup_commands.\n");
}

void cbdb_connection::process_response(void)
{
    int ret;
    bool responses_handled = false;

    struct timeval now;
    gettimeofday(&now, NULL);

    while ((ret = m_protocol->parse_response()) > 0) {
        bool error = false;
        protocol_response *r = m_protocol->get_response();

        request* req = pop_req(r->get_msg_id());
/*
        if (req->m_type == rt_set) {
            //todo implements set result
            benchmark_debug_log("set successful.\n");
        } else if (req->m_type == rt_get) {
            //todo implements get result
            benchmark_debug_log("database get successful.\n");
        } else {
*/
        benchmark_debug_log("server %s: handled response (first line): %s, %d hits, %d misses\n",
                            get_readable_id(),
                            r->get_status(),
                            r->get_hits(),
                            req->m_keys - r->get_hits());

        m_conns_manager->handle_response(m_id, now, req, r);
        m_conns_manager->inc_reqs_processed();
        responses_handled = true;
//        }
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

void cbdb_connection::process_first_request() {
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void cbdb_connection::fill_pipeline(void)
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

void cbdb_connection::handle_event(short evtype)
{
    connection::handle_event(evtype);
}

void cbdb_connection::send_wait_command(struct timeval* sent_time,
                                         unsigned int num_slaves, unsigned int timeout) {
    int cmd_size = 0;

    benchmark_debug_log("WAIT num_slaves=%u timeout=%u\n", num_slaves, timeout);

    cmd_size = m_protocol->write_command_wait(num_slaves, timeout);
    push_req(1, new request(rt_wait, cmd_size, sent_time, 0));
}

void cbdb_connection::send_set_command(struct timeval* sent_time, const char *key, int key_len,
                                        const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;
    uint64_t sequence = global_seqence.fetch_add(1);
    benchmark_debug_log("server %s: SET key=[%.*s] value_len=%u expiry=%u sequence=%u\n",
                        get_readable_id(), key_len, key, value_len, expiry, sequence);

    cmd_size = m_protocol->write_command_set(key, key_len, value, value_len,
                                             expiry, sequence);

    push_req(sequence, new request(rt_set, cmd_size, sent_time, 1));
}

void cbdb_connection::send_get_command(struct timeval* sent_time,
                                        const char *key, int key_len, unsigned int offset) {
    int cmd_size = 0;

    uint64_t sequence = global_seqence.fetch_add(1);
    benchmark_debug_log("server %s: GET key=[%.*s] sequence=%u\n",
                        get_readable_id(), key_len, key, sequence);
    cmd_size = m_protocol->write_command_get(key, key_len, sequence);

    push_req(sequence, new request(rt_get, cmd_size, sent_time, 1));
}

void cbdb_connection::send_mget_command(struct timeval* sent_time, const keylist* key_list) {
    int cmd_size = 0;

    const char *first_key, *last_key;
    unsigned int first_key_len, last_key_len;
    uint64_t sequence = global_seqence.fetch_add(1);
    first_key = key_list->get_key(0, &first_key_len);
    last_key = key_list->get_key(key_list->get_keys_count()-1, &last_key_len);

    benchmark_debug_log("MGET %d keys [%.*s] .. [%.*s]\n",
                        key_list->get_keys_count(), first_key_len, first_key, last_key_len, last_key);

    cmd_size = m_protocol->write_command_multi_get(key_list);

    push_req(sequence, new request(rt_get, cmd_size, sent_time, key_list->get_keys_count()));
}

void cbdb_connection::send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
                                               const char *value, int value_len, int expiry, unsigned int offset) {
    int cmd_size = 0;

    uint64_t sequence = global_seqence.fetch_add(1);
    benchmark_debug_log("GET key=[%.*s] value_len=%u expiry=%u\n",
                        key_len, key, value_len, expiry);

    cmd_size = m_protocol->write_command_get(key, key_len, offset);

    push_req(sequence, new verify_request(rt_get, cmd_size, sent_time, 1, key, key_len, value, value_len));
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

int cbdb_connection::send_arbitrary_command(const command_arg *arg) {
    int cmd_size = 0;

    cmd_size = m_protocol->write_arbitrary_command(arg);

    return cmd_size;
}

int cbdb_connection::send_arbitrary_command(const command_arg *arg, const char *val, int val_len) {
    int cmd_size = 0;

    if (arg->type == key_type) {
        benchmark_debug_log("key: value[%.*s]\n",  val_len, val);
    } else {
        benchmark_debug_log("data: value_len=%u\n",  val_len);
    }

    cmd_size = m_protocol->write_arbitrary_command(val, val_len);

    return cmd_size;
}

void cbdb_connection::send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size) {
    uint64_t sequence = global_seqence.fetch_add(1);
    push_req(sequence, new arbitrary_request(command_index, rt_arbitrary, cmd_size, sent_time));
}
