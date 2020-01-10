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

#include "connection.h"
#include "obj_gen.h"
#include "memtier_benchmark.h"
#include "connections_manager.h"

void cluster_client_event_handler(evutil_socket_t sfd, short evtype, void *opaque)
{
    connection *conn = (connection *) opaque;

    assert(conn != NULL);
    assert(conn->get_sockfd() == sfd);

    conn->handle_event(evtype);
}

connection::connection(unsigned int id, connections_manager* conns_man, benchmark_config* config,
                                   struct event_base* event_base, abstract_protocol* abs_protocol) :
        m_sockfd(-1), m_address(NULL), m_port(NULL), m_unix_sockaddr(NULL),
        m_event(NULL), m_pending_resp(0), m_connection_state(conn_disconnected) {
    m_id = id;
    m_conns_manager = conns_man;
    m_config = config;
    m_event_base = event_base;

    if (m_config->unix_socket) {
        m_unix_sockaddr = (struct sockaddr_un *) malloc(sizeof(struct sockaddr_un));
        assert(m_unix_sockaddr != NULL);

        m_unix_sockaddr->sun_family = AF_UNIX;
        strncpy(m_unix_sockaddr->sun_path, m_config->unix_socket, sizeof(m_unix_sockaddr->sun_path)-1);
        m_unix_sockaddr->sun_path[sizeof(m_unix_sockaddr->sun_path)-1] = '\0';
    }

    m_read_buf = evbuffer_new();
    assert(m_read_buf != NULL);

    m_write_buf = evbuffer_new();
    assert(m_write_buf != NULL);

    m_protocol = abs_protocol->clone();
    assert(m_protocol != NULL);
    m_protocol->set_buffers(m_read_buf, m_write_buf);

}
connection::~connection() {
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    if (m_address != NULL) {
        free(m_address);
        m_address = NULL;
    }

    if (m_port != NULL) {
        free(m_port);
        m_port = NULL;
    }

    if (m_unix_sockaddr != NULL) {
        free(m_unix_sockaddr);
        m_unix_sockaddr = NULL;
    }

    if (m_read_buf != NULL) {
        evbuffer_free(m_read_buf);
        m_read_buf = NULL;
    }

    if (m_write_buf != NULL) {
        evbuffer_free(m_write_buf);
        m_write_buf = NULL;
    }

    if (m_event != NULL) {
        event_free(m_event);
        m_event = NULL;
    }

    if (m_protocol != NULL) {
        delete m_protocol;
        m_protocol = NULL;
    }
}

void connection::setup_event() {
    int ret;

    if (!m_event) {
        m_event = event_new(m_event_base, m_sockfd, EV_WRITE,
                            cluster_client_event_handler, (void *)this);
        assert(m_event != NULL);
    } else {
        ret = event_del(m_event);
        assert(ret ==0);

        ret = event_assign(m_event, m_event_base, m_sockfd, EV_WRITE,
                           cluster_client_event_handler, (void *)this);
        assert(ret ==0);
    }

    ret = event_add(m_event, NULL);
    assert(ret == 0);
}

int connection::setup_socket(struct connect_info* addr) {
    int flags;

    // clean up existing socket
    if (m_sockfd != -1)
        close(m_sockfd);

    if (m_unix_sockaddr != NULL) {
        m_sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (m_sockfd < 0) {
            return -errno;
        }
    } else {
        // initialize socket
        m_sockfd = socket(addr->ci_family, addr->ci_socktype, addr->ci_protocol);
        if (m_sockfd < 0) {
            return -errno;
        }

        // configure socket behavior
        struct linger ling = {0, 0};
        int flags = 1;
        int error = setsockopt(m_sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags, sizeof(flags));
        assert(error == 0);

        error = setsockopt(m_sockfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
        assert(error == 0);

        error = setsockopt(m_sockfd, IPPROTO_TCP, TCP_NODELAY, (void *) &flags, sizeof(flags));
        assert(error == 0);
    }

    // set non-blocking behavior
    flags = 1;
    if ((flags = fcntl(m_sockfd, F_GETFL, 0)) < 0 ||
        fcntl(m_sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        benchmark_error_log("connect: failed to set non-blocking flag.\n");
        close(m_sockfd);
        m_sockfd = -1;
        return -1;
    }

    return 0;
}

int connection::connect(struct connect_info* addr) {
    // set required setup commands
    //m_authentication = m_config->authenticate ? auth_none : auth_done;
    //m_db_selection = m_config->select_db ? select_none : select_done;

    // clean up existing buffers
    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    // setup socket
    setup_socket(addr);

    // set up event
    setup_event();

    // set readable id
    set_readable_id();

    // call connect
    m_connection_state = conn_in_progress;

    if (::connect(m_sockfd,
                  m_unix_sockaddr ? (struct sockaddr *) m_unix_sockaddr : addr->ci_addr,
                  m_unix_sockaddr ? sizeof(struct sockaddr_un) : addr->ci_addrlen) == -1) {
        if (errno == EINPROGRESS || errno == EWOULDBLOCK)
            return 0;

        benchmark_error_log("connect failed, error = %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

void connection::disconnect() {
    if (m_sockfd != -1) {
        close(m_sockfd);
        m_sockfd = -1;
    }

    evbuffer_drain(m_read_buf, evbuffer_get_length(m_read_buf));
    evbuffer_drain(m_write_buf, evbuffer_get_length(m_write_buf));

    int ret = event_del(m_event);
    assert(ret == 0);

    m_connection_state = conn_disconnected;

}

void connection::set_address_port(const char* address, const char* port) {
    if (m_address != NULL) {
        free(m_address);
    }
    m_address = strdup(address);

    if (m_port != NULL) {
        free(m_port);
    }
    m_port = strdup(port);
}

void connection::set_readable_id() {
    if (m_unix_sockaddr != NULL) {
        m_readable_id.assign(m_config->unix_socket);
    } else {
        m_readable_id.assign(m_address);
        m_readable_id.append(":");
        m_readable_id.append(m_port);
    }
}

const char* connection::get_readable_id() {
    return m_readable_id.c_str();
}


void connection::process_first_request() {
    m_conns_manager->set_start_time();
    fill_pipeline();
}

void connection::handle_event(short evtype)
{
    // connect() returning to us?  normally we expect EV_WRITE, but for UNIX domain
    // sockets we workaround since connect() returned immediately, but we don't want
    // to do any I/O from the client::connect() call...
    if ((get_connection_state() != conn_connected) && (evtype == EV_WRITE || m_unix_sockaddr != NULL)) {
        int error = -1;
        socklen_t errsz = sizeof(error);

        if (getsockopt(m_sockfd, SOL_SOCKET, SO_ERROR, (void *) &error, &errsz) == -1) {
            benchmark_error_log("connect: error getting connect response (getsockopt): %s\n", strerror(errno));
            disconnect();

            return;
        }

        if (error != 0) {
            benchmark_error_log("connect: connection failed: %s\n", strerror(error));
            disconnect();

            return;
        }

        m_connection_state = conn_connected;

        if (!m_conns_manager->get_reqs_processed()) {
            process_first_request();
        } else {
            benchmark_debug_log("reconnection complete, proceeding with test\n");
            fill_pipeline();
        }
    }

    assert(get_connection_state() == conn_connected);
    if ((evtype & EV_WRITE) == EV_WRITE && evbuffer_get_length(m_write_buf) > 0) {
        if (evbuffer_write(m_write_buf, m_sockfd) < 0) {
            if (errno != EWOULDBLOCK) {
                benchmark_error_log("write error: %s\n", strerror(errno));
                disconnect();

                return;
            }
        }
    }

    if ((evtype & EV_READ) == EV_READ) {
        int ret = 1;
        while (ret > 0) {
            ret = evbuffer_read(m_read_buf, m_sockfd, -1);
        }

        if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            benchmark_error_log("read error: %s\n", strerror(errno));
            disconnect();

            return;
        }
        if (ret == 0) {
            benchmark_error_log("connection dropped.\n");
            disconnect();

            return;
        }

        if (evbuffer_get_length(m_read_buf) > 0) {
            process_response();

            // process_response may have disconnected, in which case
            // we just abort and wait for libevent to call us back sometime
            if (get_connection_state() == conn_disconnected) {
                return;
            }

        }
    }

    // update event
    short new_evtype = 0;
    if (m_pending_resp) {
        new_evtype = EV_READ;
    }

    if (evbuffer_get_length(m_write_buf) > 0) {
        assert(!m_conns_manager->finished());
        new_evtype |= EV_WRITE;
    }

    if (new_evtype) {
        int ret = event_assign(m_event, m_event_base,
                               m_sockfd, new_evtype, cluster_client_event_handler, (void *)this);
        assert(ret == 0);

        ret = event_add(m_event, NULL);
        assert(ret == 0);
    } else if (m_conns_manager->finished()) {
        m_conns_manager->set_end_time();
    }
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

int connection::send_arbitrary_command(const command_arg *arg) {
    int cmd_size = 0;

    cmd_size = m_protocol->write_arbitrary_command(arg);

    return cmd_size;
}

int connection::send_arbitrary_command(const command_arg *arg, const char *val, int val_len) {
    int cmd_size = 0;

    if (arg->type == key_type) {
        benchmark_debug_log("key: value[%.*s]\n",  val_len, val);
    } else {
        benchmark_debug_log("data: value_len=%u\n",  val_len);
    }

    cmd_size = m_protocol->write_arbitrary_command(val, val_len);

    return cmd_size;
}

void connection::send_arbitrary_command_end(size_t command_index, struct timeval* sent_time, int cmd_size) {
    
}
