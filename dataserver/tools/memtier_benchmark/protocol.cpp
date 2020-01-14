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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif
#include "protocol.h"
#include "memtier_benchmark.h"
#include "libmemcached_protocol/binary.h"

/////////////////////////////////////////////////////////////////////////

abstract_protocol::abstract_protocol() :
    m_read_buf(NULL), m_write_buf(NULL), m_keep_value(false)
{    
}

abstract_protocol::~abstract_protocol()
{
}

void abstract_protocol::set_buffers(struct evbuffer* read_buf, struct evbuffer* write_buf)
{
    m_read_buf = read_buf;
    m_write_buf = write_buf;
}

void abstract_protocol::set_keep_value(bool flag)
{
    m_keep_value = flag;
}

/////////////////////////////////////////////////////////////////////////

protocol_response::protocol_response()
    : m_status(NULL), m_mbulk_value(NULL), m_value(NULL), m_value_len(0), m_hits(0), m_error(false)
{
}

protocol_response::~protocol_response()
{
    clear();
}

void protocol_response::set_error(bool error)
{
    m_error = error;
}

bool protocol_response::is_error(void)
{
    return m_error;
}

void protocol_response::set_status(const char* status)
{
    if (m_status != NULL)
        free((void *)m_status);
    m_status = status;
}

const char* protocol_response::get_status(void)
{
    return m_status;
}

void protocol_response::set_value(const char* value, unsigned int value_len)
{
    if (m_value != NULL)
        free((void *)m_value);
    m_value = value;
    m_value_len = value_len;
}

const char* protocol_response::get_value(unsigned int* value_len)
{
    assert(value_len != NULL);

    *value_len = m_value_len;
    return m_value;
}

void protocol_response::set_total_len(unsigned int total_len)
{
    m_total_len = total_len;
}

unsigned int protocol_response::get_total_len(void)
{
    return m_total_len;
}

void protocol_response::incr_hits(void)
{
    m_hits++;
}

unsigned int protocol_response::get_hits(void)
{
    return m_hits;
}

void protocol_response::set_msg_id(uint64_t msg_id)
{
    m_msg_id = msg_id;
}

uint64_t protocol_response::get_msg_id(void)
{
    return m_msg_id;
}

void protocol_response::clear(void)
{
    if (m_status != NULL) {
        free((void *)m_status);
        m_status = NULL;
    }
    if (m_value != NULL) {
        free((void *)m_value);
        m_value = NULL;
    }
    if (m_mbulk_value != NULL) {
        delete m_mbulk_value;
        m_mbulk_value = NULL;
    }

    m_value_len = 0;
    m_total_len = 0;
    m_hits = 0;
    m_error = 0;
    m_msg_id = 0;
}

void protocol_response::set_mbulk_value(mbulk_size_el* element) {
    m_mbulk_value = element;
}

mbulk_size_el* protocol_response::get_mbulk_value() {
    return m_mbulk_value;
}


/////////////////////////////////////////////////////////////////////////

keylist::keylist(unsigned int max_keys) :
    m_buffer(NULL), m_buffer_ptr(NULL), m_buffer_size(0),
    m_keys(NULL), m_keys_size(0), m_keys_count(0)
{
    m_keys_size = max_keys;
    m_keys = (key_entry *) malloc(m_keys_size * sizeof(key_entry));
    assert(m_keys != NULL);
    memset(m_keys, 0, m_keys_size * sizeof(key_entry));

    /* allocate buffer for actual keys */
    m_buffer_size = 256 * m_keys_size;
    m_buffer = (char *) malloc(m_buffer_size);
    assert(m_buffer != NULL);
    memset(m_buffer, 0, m_buffer_size);

    m_buffer_ptr = m_buffer;
}

keylist::~keylist()
{
    if (m_buffer != NULL) {
        free(m_buffer);
        m_buffer = NULL;
    }
    if (m_keys != NULL) {
        free(m_keys);
        m_keys = NULL;
    }
}

bool keylist::add_key(const char *key, unsigned int key_len)
{
    // have room?
    if (m_keys_count >= m_keys_size)
        return false;

    // have buffer?
    if (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
        while (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
            m_buffer_size *= 2;
        }
        m_buffer = (char *)realloc(m_buffer, m_buffer_size);
        assert(m_buffer != NULL);
    }

    // copy key
    memcpy(m_buffer_ptr, key, key_len);
    m_buffer_ptr[key_len] = '\0';
    m_keys[m_keys_count].key_ptr = m_buffer_ptr;
    m_keys[m_keys_count].key_len = key_len;

    m_buffer_ptr += key_len + 1;
    m_keys_count++;

    return true;
}

unsigned int keylist::get_keys_count(void) const
{
    return m_keys_count;
}

const char *keylist::get_key(unsigned int index, unsigned int *key_len) const
{
    if (index < 0 || index >= m_keys_count)
        return NULL;
    if (key_len != NULL)
        *key_len = m_keys[index].key_len;
    return m_keys[index].key_ptr;
}

void keylist::clear(void)
{
    m_keys_count = 0;
    m_buffer_ptr = m_buffer;
}