#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif
#include <iostream>
#include <iomanip>
#include "net/message.h"
#include "net/protocol.h"
#include "dspb/kv.pb.h"
#include "dspb/api.pb.h"
#include "dspb/function.pb.h"
#include "dspb/error.pb.h"
#include "cbdb_protocol.h"

using namespace jim::net;

int cbdb_protocol::select_db(int db)
{
    int size = 0;
    return size;
}

int cbdb_protocol::authenticate(const char *credentials)
{
    int size = 0;
    return size;
}

int cbdb_protocol::write_command_cluster_slots()
{
    int size = 0;
    return size;
}
/*
 * note: offset is sequence
 */
int cbdb_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;
    dspb::RangeRequest proto_req;
    auto header = proto_req.mutable_header();
    header->set_cluster_id(1);
    header->set_trace_id(offset);
    header->set_range_id(1); //default is set to 1
    header->mutable_range_epoch()->set_conf_ver(1); //default is set to 1
    header->mutable_range_epoch()->set_version(1); //default is set to 1
    proto_req.mutable_kv_put()->set_key(key, key_len);
    proto_req.mutable_kv_put()->set_value(value, value_len);
    auto msg = NewMessage();
    msg->head.msg_type = kDataRequestType;
    msg->head.msg_id = offset;
    msg->head.func_id = dspb::kFuncRangeRequest;
    msg->head.body_length = proto_req.ByteSizeLong();
    msg->body.resize(msg->head.body_length);
    auto ret = proto_req.SerializeToArray(msg->body.data(), static_cast<int>(msg->head.body_length));
    if (ret) {
        msg->head.Encode();
        size = evbuffer_add(m_write_buf, (void*)&msg->head, sizeof(msg->head));
        size += evbuffer_add(m_write_buf, msg->body.data(), msg->body.size());
    } else {
        fprintf(stderr, "error: serialize body failed\n");
    }
    return size;
}

int cbdb_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for redis yet!\n");
    assert(0);
}

int cbdb_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;
    dspb::RangeRequest proto_req;
    auto header = proto_req.mutable_header();
    header->set_cluster_id(1);
    header->set_trace_id(offset);
    header->set_range_id(1); //default is set to 1
    header->mutable_range_epoch()->set_conf_ver(1); //default is set to 1
    header->mutable_range_epoch()->set_version(1); //default is set to 1
    proto_req.mutable_kv_get()->set_key(key, key_len);

    auto msg = NewMessage();
    msg->head.msg_type = kDataRequestType;
    msg->head.msg_id = offset;
    msg->head.func_id = dspb::kFuncRangeRequest;
    msg->head.body_length = proto_req.ByteSizeLong();
    msg->body.resize(msg->head.body_length);

    auto ret = proto_req.SerializeToArray(msg->body.data(), static_cast<int>(msg->head.body_length));
    if (ret) {
        msg->head.Encode();
        size = evbuffer_add(m_write_buf, (void*)&msg->head, sizeof(msg->head));
        size += evbuffer_add(m_write_buf, msg->body.data(), msg->body.size());
    } else {
        fprintf(stderr, "error: serialize body failed\n");
    }
    return size;
}

int cbdb_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    int size = 0;
    return size;
}

int cbdb_protocol::parse_response(void)
{
    int ret = -1;
    while (true) {
        switch (m_response_state) {
            case rs_initial:
                if (evbuffer_get_length(m_read_buf) < sizeof(m_response_hdr)) {
                    return 0; // no header yet or read uncomplete?
                }

                ret = evbuffer_remove(m_read_buf, (void *)&m_response_hdr, sizeof(m_response_hdr));
                assert(ret == sizeof(m_response_hdr));
                m_response_hdr.Decode();
                if (m_response_hdr.magic != kMagic) {
                    benchmark_error_log("error: invalid cbdb response header magic.\n");
                    return -1;
                }
                if (m_response_hdr.msg_type != kDataResponseType) {
                    benchmark_error_log("error: invalid cbdb response header msg_type: %d.\n", m_response_hdr.msg_type);
                    return -1;
                }
                m_response_len = sizeof(m_response_hdr);
                // clear last response
                m_last_response.clear();
                m_last_response.set_msg_id(m_response_hdr.msg_id);
                //benchmark_error_log("msg_id: %lld, body_length: %d.\n", m_response_hdr.msg_id, m_response_hdr.body_length);
                if (m_response_hdr.body_length > 0) {
                    m_response_state = rs_read_body;
                    continue;
                }
                return 1;
                //break;
            case rs_read_body:
                if (evbuffer_get_length(m_read_buf) >= m_response_hdr.body_length) {
                    char tmp[1024];
                    ret = evbuffer_remove(m_read_buf, (void*)tmp, m_response_hdr.body_length);
                    dspb::RangeResponse proto_resp;
                    auto bsucc = proto_resp.ParseFromArray(tmp, m_response_hdr.body_length);
                    if (bsucc) {
                        switch (proto_resp.resp_case()) {
                        case dspb::RangeResponse::kKvGet:
                        {
                            if (proto_resp.header().has_error()) {
                                benchmark_error_log("get error: %s\n", proto_resp.header().error().ShortDebugString().c_str());
                                return -1;
                            }
                            auto resp_get = proto_resp.kv_get();
                            if (resp_get.code()) {
                                benchmark_error_log("error code: %d\n", resp_get.code());
                            } else {
                                if (m_keep_value) {
                                    m_last_response.set_value(resp_get.value().c_str(), resp_get.value().length());
                                }
                                m_last_response.incr_hits();
                            }
                        }
                            break;
                        case dspb::RangeResponse::kKvPut:
                        {
                            if (proto_resp.header().has_error()) {
                                benchmark_error_log("get error: %s\n", proto_resp.header().error().ShortDebugString().c_str());
                                return -1;
                            }
                            auto resp = proto_resp.kv_put();
                            if (resp.code()) {
                                benchmark_error_log("error code: %d\n", resp.code());
                                return -1;
                            }
                        }
                            break;
                        default:
                            benchmark_error_log("not expected response: %d\n", static_cast<int>(proto_resp.resp_case()));
                        }
                    } else {
                        benchmark_error_log("decode message failed from stream\n");
                        return -1;
                    }
/*
                    if (m_response_hdr.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS)
                        m_last_response.incr_hits();
*/
                    m_response_len += m_response_hdr.body_length;
                    m_response_state = rs_initial;

                    return 1;
                } else {
                    return 0;
                }
                break;
            default:
                benchmark_error_log("unkown response state: %d.\n", m_response_state);
                return -1;
        }
    }

    return -1;
}

int cbdb_protocol::write_arbitrary_command(const command_arg *arg) {
    evbuffer_add(m_write_buf, arg->data.c_str(), arg->data.length());

    return arg->data.length();
}

int cbdb_protocol::write_arbitrary_command(const char *rand_val, int rand_val_len) {
    int size = 0;

    size = evbuffer_add_printf(m_write_buf, "$%d\r\n", rand_val_len);
    evbuffer_add(m_write_buf, rand_val, rand_val_len);
    size += rand_val_len;
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += 2;

    return size;
}

bool cbdb_protocol::format_arbitrary_command(arbitrary_command &cmd) {
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
