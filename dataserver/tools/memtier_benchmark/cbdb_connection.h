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

#ifndef MEMTIER_BENCHMARK_CBDB_CONNECTION_H
#define MEMTIER_BENCHMARK_CBDB_CONNECTION_H

#include <queue>
#include <string>
#include <netdb.h>
#include <map>
#include <sys/socket.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/buffer.h>

#include "protocol.h"
#include "request.h"
#include "connection.h"
// forward decleration
struct benchmark_config;
class object_generator;

class cbdb_connection : public connection {

public:
    cbdb_connection(unsigned int id, connections_manager* conn_man, benchmark_config* config,
                     struct event_base* event_base, abstract_protocol* abs_protocol);
    ~cbdb_connection();

    void set_authentication() {

    }

    void set_select_db() {

    }

    void set_cluster_slots() {

    }

    enum cluster_slots_state get_cluster_slots_state() {
        return slots_none;
    }

    bool is_conn_setup_done() override;
    void send_conn_setup_commands(struct timeval timestamp) override;
    void fill_pipeline(void) override;
    void process_response(void) override;
    void process_first_request() override;
    void handle_event(short evtype);

    void send_wait_command(struct timeval* sent_time,
            unsigned int num_slaves, unsigned int timeout) override;

    void send_set_command(struct timeval* sent_time, const char *key, int key_len,
            const char *value, int value_len, int expiry, unsigned int offset) override;

    void send_get_command(struct timeval* sent_time,
            const char *key, int key_len, unsigned int offset) override;

    void send_mget_command(struct timeval* sent_time, const keylist* key_list) override;

    void send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
            const char *value, int value_len, int expiry, unsigned int offset) override;

    int send_arbitrary_command(const command_arg *arg) override;
    int send_arbitrary_command(const command_arg *arg, const char *val, int val_len) override;
    void send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size) override;
private:
    void push_req(int msg_id, request* req);
    request* pop_req(int msg_id);
    std::map<int, request *>* m_pipeline;
};

#endif //MEMTIER_BENCHMARK_CBDB_CONNECTION_H
