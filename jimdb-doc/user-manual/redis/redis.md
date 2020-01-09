# chubao redis proxy

## chubao redis 

proxy是一款完全兼容redis功能的服务端,目前实现了redis的部分功能,能够通过官方redis客户端(redis_cli)和sdk访问服务端,当前版本主要支持string和hash两种数据结构,支持命令如下:

## 数据库命令

### del
del key [...]
> 删除指定的一个key和多个key, 时间复杂度为O(n), n为被删除的key的数量, 不存在的key会被忽略

*返回值*
> 被删除key的数量

## 字符串命令

### get
get key
> 返回与键key向关联的字符串值, 时间复杂度为O(1).

*返回值*
> 如果key不存在,那么返回特殊值 nil, 否则返回键key的值.
> 如果键key的值并非字符串类型,那么返回一个错误.

### set
set key value
> 将字符串值value关联到键key, 如果key已经关联其他值,则覆盖写,无论以前是什么类型.

*返回值*
> 成功返回OK, 失败会返回错误.

### mget
mget key [key...]
> 返回给定的一个和多个字符串值
> 如果给定的字符串键里面,某个键不存在,则这个键对应的值为特殊值: nil.

*返回值*
> mget将返回一个列表,列表中包含了所有给定键的值

### mset
mset key value [[key value]...]
> 同时为多个键设置值
> 如果某个给定键已经存在,那么mset将用新值去覆盖旧值

*返回值*
> mset总是返回OK

## hash命令

### hget
hget key field
> 返回哈稀表中给定域的值

*返回值*
> hget返回hash键中给定域的值
> 如果给定域不存在于hash键中,或者给定的hash键不存在,返回特殊值: nil.

### hset
hset hash field value
> 将哈稀表中field的值设置为value
> 如果给定的哈稀表不存在,那么一个新的哈稀表会被创建,然后执行hset操作
> 如果field已经存在于哈稀表中, 则用新值覆盖旧值.

*返回值*
> 当 hset 命令在哈稀表中创建新的域 field 并成功设置值时,返回 1, 如果域 field 已经存在于哈稀表中, 并且 hset 命令成功用新值覆盖了旧值, 返回 0.


### hmset
hmset hash field value [field value ...]
> 同时将多个键-值对设置到哈稀表中, 时间复杂度为O(n).
> 此命令会覆盖哈稀表中已存在的域.
> 如果哈稀表不存在,会创建哈稀表,然后执行hmset命令.

*返回值*
> 如果命令执行成功, 返回OK, 当key不是hash类型时,返回错误

### hmget
hmget hash field [field ...]
> 返回哈稀表中一个或多个域的值
> 如果给定的域不存在于哈稀表中, 返回 nil
> 如果 哈稀表不存在, 则被当作一个空的哈稀表处理,会返回一个只带有 nil 的表.

*返回值*
> 一个包含多个给定域的关连值的表, 表值的顺序与给定域的顺序一致.

### hgetall
hgetall key
> 返回哈稀表中所有的域和值
> 在*返回值*里, 紧跟每隔域名后时该域的值

*返回值*
> 以列表形式返回哈稀表中的域和值, 若key不存在,返回空列表.



