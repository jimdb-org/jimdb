Redis Proxy
=============================

Redis Proxy
--------------------------

Redis Proxy is a server fully compatible with redis functions. At present, it has realized some redis functions and can access the server through the official redis client (redis_cli) and SDK. The current version mainly supports string and hash data structures. The following commands are supported:


Database Command
--------------------------

**del**
 * del key [...]
 
 Removes the specified keys. the individual complexity for the key is O(n) where n is the number of keys. A key is ignored if it does not exist.

 * Return value

 Integer reply: The number of keys that were removed


String Command
--------------------------

**get**
 * get key
 
 Returns the string value associated with the direction of the key, The time complexity is O(1).

 * Return value

 If the key does not exist, return the special value nil, otherwise return the value of the key. If the value of the key is not of string type, an error is returned

**set**
 * set key value

 Associate the string value with the key, overwrite if the key is already associated with other values, regardless of the previous type.

 * Return value

 Success: return OK; failure: return an error.

**mget**
 * mget key [key...]

Returns a given one or more string values. If a key does not exist in a given string key, the value for that key is the special value: nil.

 * Return value

Mget returns a list of all the values for a given key

**mset**
 * mset key value [[key value]...]

 Sets values for multiple keys at the same time, and if a given key already exists, the mset overwrites the old value with the new value

 * Return value
 
 Mset always returns OK

Hash Command
--------------------------

**hget**
 * hget key field

 Returns the value of the given field in the hash table

 * Return value
 
 Hget returns the value of the given field in the Hash table,  if the given hash key does not exist, returns the special value: nil

**hset**
 * hset hash field value

 Sets the value of field in the hash table. If the given Hash table does not exist, a new hash table is created, Then execute the hset command. If the field already exists in the Hash table, the old value is overwritten with the new value

 * Return value

 When the hset command creates a new field in the hash table and successfully sets the value, return 1. if the field already exists in the hash table and the hset command successfully overwrites the old value with the new value, return 0.

**hmset**
 * hmset hash field value [field value ...]

 sets multiple key-value pairs into the hash table at the same time, The time complexity is O(n). This command overrides the existing fields in the hash table. If the hash table does not exist, the hash table is created and the hmset command is executed.

 * Return value

 Success: return OK, Error returned when key is not of hash type.

**hmget**
 * hmget hash field [field ...]

 Returns the value of one or more fields in a hash table. If the given field does not exist in the Hash table, return nil. If the hash table does not exist, it is treated as an empty hash table and a table with only nil is returned

 * Return value

 A table containing the associated values for a given number of fields, Table values are in the same order as the given field

**hgetall**
 * hgetall key

 Returns all fields and values in the hash table, In the return value, the value of the field immediately after each interval

 * Return value

 Returns a list of fields and values in a hash table, Returns an empty list if the key does not exist
