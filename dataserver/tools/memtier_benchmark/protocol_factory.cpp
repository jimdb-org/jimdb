#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "protocol_factory.h"
#include "redis_protocol.h"
#include "memcached_protocol.h"
#include "cbdb_protocol.h"
class abstract_protocol *protocol_factory(const char *proto_name)
{
    assert(proto_name != NULL);

    if (strcmp(proto_name, "redis") == 0) {
        return new redis_protocol();
    } else if (strcmp(proto_name, "memcache_text") == 0) {
        return new memcache_text_protocol();
    } else if (strcmp(proto_name, "memcache_binary") == 0) {
        return new memcache_binary_protocol();
    } else if (strcmp(proto_name, "cbdb") == 0) {
        return new cbdb_protocol();
    } else {
        benchmark_error_log("Error: unknown protocol '%s'.\n", proto_name);
        return NULL;
    }
}
