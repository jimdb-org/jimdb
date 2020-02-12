# 统计信息简介

SQL 优化器会根据统计信息来选择最优的执行计划。统计信息收集了表级别和列级别的信息，表的统计信息包括总行数，以及修改的行数。列的统计信息包括不同值的数量，NULL 的数量，以及该列的直方图信息。

## 统计信息的收集

### 手动收集

我们可以通过执行 `ANALYZE` 语句来收集统计信息。

语法：
```sql
ANALYZE TABLE table
> 该语句会收集 table 中所有表的统计信息。
```

### 自动更新

目前统计信息会定期在后台做全量更新。更新周期的默认值是 10s，如果将其指定为 0，那么将不会自动更新。

未来我们会支持在发生增加，删除以及修改语句时自动更新表的总行数以及修改的行数。这些信息会定期持久化下来，

## 统计信息的查看

### 表的元信息

目前不支持通过 `SHOW STATS_META` 来查看表的总行数以及修改的行数等信息。

语法：
```sql
SHOW STATS_META table
> 该语句会输出所有表的总行数以及修改行数等信息。
```

`SHOW STATS_META` 输出：

| 语法元素 | 说明            |
| -------- | ------------- |
| db_name  |  数据库名    |
| table_name | 表名 |
| update_time | 更新时间 |
| modify_count | 修改的行数 |
| row_count | 总行数 |

### 列的元信息

目前不支持通过 `SHOW STATS_HISTOGRAMS` 来查看列的不同值数量以及 NULL 数量等信息。

语法：
```sql
SHOW STATS_HISTOGRAMS table
> 该语句会输出所有列的不同值数量以及 NULL 数量等信息。
```

`SHOW STATS_HISTOGRAMS` 输出：

| 语法元素 | 说明            |
| -------- | ------------- |
| db  |  数据库名    |
| table | 表名 |
| column | 列名 |
| is_index | 是否是索引列 |
| update_time | 更新时间 |
| distinct_count | 不同值数量 |
| null_count | NULL 的数量 |

### 直方图桶的信息

目前不支持通过 `SHOW STATS_BUCKETS` 来查看直方图每个桶的信息。

语法：
```sql
SHOW STATS_BUCKETS table
> 该语句会输出所有桶的信息。
```

`SHOW STATS_BUCKETS` 输出：

| 语法元素 | 说明            |
| -------- | ------------- |
| db  |  数据库名    |
| table | 表名 |
| column | 列名 |
| is_index | 是否是索引列 |
| bucket_id | 桶的编号 |
| count | 所有落在这个桶及之前桶中值的数量 |
| repeats | 最大值出现的次数 |
| lower_bound | 最小值 |
| upper_bound | 最大值 |

## 删除统计信息

目前不支持删除统计信息.

语法：
```sql
DROP STATS table
> 该语句会删除 table 中所有的统计信息。
```
