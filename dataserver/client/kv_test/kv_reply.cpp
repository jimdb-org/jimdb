// Copyright 2019 The JIMDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.


#include "kv_reply.h"

#include "jim_kv_common.h"
#include "jim_log.h"

void print_reply(KVReplyPtr reply) {
    if (reply == nullptr) {
        CBLOG_ERROR("can not get reply");
        return;
    }
    switch (reply->type()) {
    case JimKVReplyType::REPLY_STRING:    /* fall-thru */
    case JimKVReplyType::REPLY_STATUS:    /* fall-thru */
    case JimKVReplyType::REPLY_ERROR:     /* fall-thru */
        CBLOG_INFO("result: %s", reply->buf().c_str());
        break;
    case JimKVReplyType::REPLY_INTEGER:
        CBLOG_INFO("result: %" PRIu64, reply->integer());
        break;
    case JimKVReplyType::REPLY_NIL:
        CBLOG_INFO("result: null");
        break;
    case JimKVReplyType::REPLY_ARRAY:
        {
            auto &elements = reply->elements();
            for (auto &e : elements) {
                print_reply(e);
            }

            break;
        }
    case JimKVReplyType::REPLY_MAX:
        CBLOG_ERROR("append pipeline max command");
        break;
    default:
        CBLOG_ERROR("reply wrong type");
        break;
    }
}
 
