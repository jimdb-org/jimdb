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

#ifndef MEMTIER_BENCHMARK_SHARD_CONNECTION_H
#define MEMTIER_BENCHMARK_SHARD_CONNECTION_H

#include <queue>
#include <string>
#include <netdb.h>
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

//enum authentication_state { auth_none, auth_sent, auth_done };
//enum select_db_state { select_none, select_sent, select_done };
//enum cluster_slots_state { slots_none, slots_sent, slots_done };

class shard_connection : public connection {

public:
    shard_connection(unsigned int id, connections_manager* conn_man, benchmark_config* config,
                     struct event_base* event_base, abstract_protocol* abs_protocol);
    ~shard_connection();

    void set_authentication() {
        m_authentication = auth_none;
    }

    void set_select_db() {
        m_db_selection = select_none;
    }

    void set_cluster_slots() {
        m_cluster_slots = slots_none;
    }

    enum cluster_slots_state get_cluster_slots_state() {
        return m_cluster_slots;
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
    void push_req(request* req);
    request* pop_req();
    std::queue<request *>* m_pipeline;
    enum authentication_state m_authentication = auth_done;
    enum select_db_state m_db_selection = select_done;
    enum cluster_slots_state m_cluster_slots = slots_done;
};

#endif //MEMTIER_BENCHMARK_SHARD_CONNECTION_H
