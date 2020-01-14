#include "protocol.h"
#include "libmemcached_protocol/binary.h"

class memcache_text_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_section, rs_read_value, rs_read_end };
    response_state m_response_state;
    unsigned int m_value_len;
    size_t m_response_len;
public:
    memcache_text_protocol() : m_response_state(rs_initial), m_value_len(0), m_response_len(0) { }
    virtual memcache_text_protocol* clone(void) { return new memcache_text_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);

};

class memcache_binary_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_body };
    response_state m_response_state;
    protocol_binary_response_no_extras m_response_hdr;
    size_t m_response_len;

    const char* status_text(void);
public:
    memcache_binary_protocol() : m_response_state(rs_initial), m_response_len(0) { }
    virtual memcache_binary_protocol* clone(void) { return new memcache_binary_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);
};
