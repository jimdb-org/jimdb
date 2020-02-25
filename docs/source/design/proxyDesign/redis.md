# Redis Proxy

## Overall architecture

AP is designed as a TCP server compatible with redis protocol, which is responsible for receiving commands from native redis clients, then calling the JIMDB SDK to interact with data server, write or read data, and convert data returned by data server into redis protocol and return to clients Complete redis command processing, the overall process as follows:


 ![Structure](../../images/redis-proxy-structure.png)

 
use redis's auth command to wrap up the relevant information of JIMDB (cluster_id, db_id, table_id) when establishing the link , complete the initialization of the JIMDB Sdk, and then read and write. All kinds of commands are compatible with redis semantics as far as possible

## Support types

Currently only string and hash data structures are supported, and the following command processing is possible:
* set key value

* get key

* del key

* mset key1 value1 key2 value2 ....

* mget key1 key2 ...

* hset hash field value 

* hget hash field

* hmset hash field1 value1 field2 value2 ...*

* hmget hash field1 field2 ...

* hdel hash field1 field2 ...

* hgetall hash

## CODEC scheme

### string KEY
  
  The first byte type distinguishes between different data types; (see: Data Type Convention)
  The 2～3 bytes are the hash value of the key; The main purpose of Hashes is to scatter the keys and the shards

### string VALUE

  The first byte flag is used to mark the key deletion (0 Normal 1 deletion);
  (The deletion process of non-string key is rather lengthy, marking before deleting)
  The 2 ~ 5 byte TTL holds the time-out information for the key. 0 indicates that the expiration time is not set, If the value is greater than 0, the expiration time is set in ms.
  Finally, the original Value content.

### Hash type processing scheme

The Hash is split into multiple keys at the AP layer, and the original key and field are combined into a single key to be stored in the data server

