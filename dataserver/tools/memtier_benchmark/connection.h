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

#ifndef MEMTIER_BENCHMARK_CONNECTION_H
#define MEMTIER_BENCHMARK_CONNECTION_H

#include <queue>
#include <string>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include "request.h"
#include "protocol.h"
void cluster_client_event_handler(evutil_socket_t sfd, short evtype, void *opaque);
// forward decleration
enum connection_state { conn_disconnected, conn_in_progress, conn_connected };
class connections_manager;
enum authentication_state { auth_none, auth_sent, auth_done };
enum select_db_state { select_none, select_sent, select_done };
enum cluster_slots_state { slots_none, slots_sent, slots_done };
/*
struct benchmark_config;
class abstract_protocol;
class object_generator;

enum connection_state { conn_disconnected, conn_in_progress, conn_connected };

enum authentication_state { auth_none, auth_sent, auth_done };
enum select_db_state { select_none, select_sent, select_done };
enum cluster_slots_state { slots_none, slots_sent, slots_done };

enum request_type { rt_unknown, rt_set, rt_get, rt_wait, rt_arbitrary, rt_auth, rt_select_db, rt_cluster_slots };
*/
class connection {
public:
    connection(unsigned int id, connections_manager* conn_man, benchmark_config* config,
                     struct event_base* event_base, abstract_protocol* abs_protocol);

    virtual ~connection();

    void set_address_port(const char* address, const char* port);
    const char* get_readable_id();
    void handle_event(short evtype);

    int connect(struct connect_info* addr);
    void disconnect();

    virtual void send_wait_command(struct timeval* sent_time,
                            unsigned int num_slaves, unsigned int timeout) = 0;
    virtual void send_set_command(struct timeval* sent_time, const char *key, int key_len,
                          const char *value, int value_len, int expiry, unsigned int offset) = 0;
    virtual void send_get_command(struct timeval* sent_time,
                          const char *key, int key_len, unsigned int offset) = 0;
    virtual void send_mget_command(struct timeval* sent_time, const keylist* key_list) = 0;
    virtual void send_verify_get_command(struct timeval* sent_time, const char *key, int key_len,
                                 const char *value, int value_len, int expiry, unsigned int offset) = 0;
    virtual int send_arbitrary_command(const command_arg *arg);
    virtual int send_arbitrary_command(const command_arg *arg, const char *val, int val_len);
    virtual void send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size);

    virtual void set_authentication() = 0;
    virtual void set_select_db() = 0;
    virtual void set_cluster_slots() = 0;

    virtual enum cluster_slots_state get_cluster_slots_state() = 0;
    virtual unsigned int get_id() {
        return m_id;
    }

    virtual int get_sockfd() {
        return m_sockfd;
    }

    virtual unsigned int get_pipeline_size() {
        return m_pipeline_size;
    }

    virtual abstract_protocol* get_protocol() {
        return m_protocol;
    }

    virtual const char* get_address() {
        return m_address;
    }

    virtual const char* get_port() {
        return m_port;
    }

    virtual enum connection_state get_connection_state() {
        return m_connection_state;
    }

    virtual void process_response(void) = 0;
    virtual bool is_conn_setup_done() = 0;

    void setup_event();
    int setup_socket(struct connect_info* addr);
    void set_readable_id();


    virtual void process_first_request();
    virtual void send_conn_setup_commands(struct timeval timestamp) = 0;
    virtual void fill_pipeline(void) = 0;


protected:
    unsigned int m_id;
    connections_manager* m_conns_manager;
    benchmark_config* m_config;

    int m_sockfd;
    char* m_address;
    char* m_port;
    std::string m_readable_id;

    struct sockaddr_un* m_unix_sockaddr;

    struct evbuffer* m_read_buf;
    struct evbuffer* m_write_buf;

    struct event_base* m_event_base;
    struct event* m_event;

    abstract_protocol* m_protocol;

    int m_pending_resp;

    unsigned int m_pipeline_size;
    enum connection_state m_connection_state;
/*
    std::queue<request *>* m_pipeline;
    enum authentication_state m_authentication;
    enum select_db_state m_db_selection;
    enum cluster_slots_state m_cluster_slots;
*/
};

#endif //MEMTIER_BENCHMARK_CONNECTION_H
