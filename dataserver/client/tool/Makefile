.SUFFIXES: .c .lo

COMPILE = $(CC) -Wall -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -g -O0 -DDEBUG_FLAG -fPIC
INC_PATH = -I../src
LIB_PATH = -L../build -ljimkv-sdk -rdynamic -lm -lcjson -lpthread -ldl
TARGET_LIB = /usr/lib64
TARGET_PATH = /usr/bin
CONFIG_PATH = /etc/mc

ALL_PRGS = batch_stress

all: $(ALL_PRGS)

batch_stress: batch_stress.lo bs_common.lo bs_string.lo bs_hash.lo bs_list.lo bs_set.lo bs_keys.lo
	$(COMPILE) -o $@ $^ $(LIB_PATH)

.lo:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(LIB_PATH) $(INC_PATH)
.c.lo:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
install:

clean:
	rm -f *.lo $(ALL_PRGS)


