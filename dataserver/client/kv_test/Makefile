.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -g -O0
INC_PATH = -I../kv
# LIB_PATH = -rdynamic ../../build/client/libjimkv-sdk.a ../../build/src/base/libjim-base.a ../../build/src/common/libjimkv-common.a ../../build/libjim-proto.a ../../build/src/net/libjim-net.a ../../.external/protobuf/libprotobuf.a ../../third-party/cpr/build/lib/libcpr.a ../../.external/jemalloc/lib/libjemalloc.a -lcurl -lstdc++ -lpthread -lm -ldl
LIB_PATH = -rdynamic ../../build/client/libjimkv-sdk.a ../../build/src/base/libjim-base.a ../../build/src/common/libjimkv-common.a ../../build/libjim-proto.a ../../build/src/net/libjim-net.a ../../.external/protobuf/libprotobuf.a ../../third-party/cpr/build/lib/libcpr.a -lcurl -lstdc++ -lpthread -lm -ldl
TARGET_LIB = /usr/lib64
TARGET_PATH = /usr/bin
CONFIG_PATH = /etc/mc

ALL_PRGS = jimkv_test

all: $(ALL_PRGS)

jimkv_test: jimkv_test.o
	$(COMPILE) -o $@ $^ $(LIB_PATH)

.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) $(INC_PATH) -c -o $@ $<
install:

clean:
	rm -f *.o $(ALL_PRGS)


