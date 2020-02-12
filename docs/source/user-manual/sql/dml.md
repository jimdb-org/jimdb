# 数据操作语言

数据操作语言（Data Manipulation Language， DML）用于帮助用户实现对数据库的基本操作，比如查询, 写入, 删除和修改数据库中的数据。

目前我们支持的数据操作语言包括 Select, Insert, Delete 和 Update。



## Select 语句

Select 语句用于从数据库中查询数据。目前我们不支持函数 limit, count, group等函数（limit、count等正在开发中)。并且我们目前仅支持单表的简单查询，不支持多表查询。

### 语法定义

```sql
SELECT expression [, expression ...]
    [FROM table
    [WHERE condition]
    [GROUP BY column_name
      [ASC | DESC], ...]
    [HAVING condition]
    [ORDER BY column_name
      [ASC | DESC], ...]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
```

### 语法元素说明

|语法元素 | 说明 |
| --------------------- | -------------------------------------------------- |
|`expression` | 投影操作列表，一般包括列名、表达式，或者是用 '\*' 表示全部列|
|`FROM table` | 表示数据来源，数据来源可以是一个表（`select * from t;`）或者是0个表 (`select 1+1 from dual;`, 等价于 `select 1+1;`)|
|`WHERE condition` | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据|
|`GROUP BY` | GroupBy 子句用于对查询结果集进行分组|
|`HAVING condition` | Having 子句与 Where 子句作用类似，Having 子句可以让过滤 GroupBy 后的各种数据，Where 子句用于在聚合前过滤记录。|
|`ORDER BY` | OrderBy 子句用于指定结果排序顺序，可以按照列表中某个位置的字段进行排序。|
|`LIMIT` | Limit 子句用于限制结果条数。Limit 接受一个或两个数字参数，如果只有一个参数，那么表示返回数据的最大行数；如果是两个参数，那么第一个参数表示返回数据的第一行的偏移量（第一行数据的偏移量是 0），第二个参数指定返回数据的最大条目数。|



## Insert 语句

Insert 语句用于向数据库中插入数据。  

### 语法定义

```sql
INSERT INTO 
    table [(column_name [, column_name] ...)] 
    VALUES (column_value, [, column_value] ...) [, (column_value, [, column_value] ...)] ...

```

### 语法元素说明

| 语法元素 | 说明 |
| -------------- | --------------------------------------------------------- |
| `table` | 要插入的表名 |
| `column_name` | 要插入的列名|
| `column_value` | 待插入的列数据 |

待插入的数据集，可以用以下三种方式指定：

* 整行插入, 例如：

```sql
INSERT INTO user VALUES ('tom', 20, 'male'), ('lucy', 19, 'female');
```

上面的例子中，`('tom', 20, 'male'), ('lucy', 19, 'female')` 中每个括号内部的数据表示一行数据，这个例子中插入了两行数据。

* 部分列插入, 例如：

```sql
INSERT INTO user (name, age) VALUES ('tom', 20), ('lucy', 19);
```

上面的例子中，每行数据只指定了 name 和 age 这两列的值，gender 列的值会设为 null。



## Delete 语句

Delete 语句用于删除数据库中的数据，目前我们仅支持单表有条件的删除。

### 语法定义

```sql
DELETE FROM table
    [WHERE condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

### 语法元素说明

| 语法元素 | 说明 |
| -------------- | --------------------------------------------------------- |
| `table` | 要删除数据的表名 |
| `WHERE condition` | `WHERE` 表达式，只删除满足表达式的行 |
| `ORDER BY` | 对待删除数据集进行排序 |
| `LIMIT row_count` | 只对待删除数据集中排序前 row_count 行的内容进行删除 |



## Update 语句

Update 语句用于更新表中的数据。目前我们支持单表有条件的更新并且 `SET` 不支持 +/- 操作。我们目前不支持无条件的更新或者多表更新。

### 语法定义

```sql
UPDATE table
    SET assignment_list
    [WHERE condition]
    [ORDER BY ...]
    [LIMIT row_count]

assignment:
    colum_name = column_value

assignment_list:
    assignment [, assignment] ...
```

Update 语句更新指定表中现有行的指定列。`SET assignment_list` 指定了要更新的列名，以及要赋予的新值。 Where/OrderBy/Limit 子句一起用于从表中查询出待更新的数据。


### 语法元素说明

| 语法元素 | 说明 |
| -------------- | --------------------------------------------------------- |
| `table` | 待更新的 Table 名称 |
| `SET assignment_list` | 待更新的列名以及目标值 |
| `WHERE condition` | `WHERE` 表达式，只更新满足表达式的那些行 |
| `ORDER BY` | 对待更新数据集进行排序 |
| `LIMIT row_count` | 只对待更新数据集中排序前 row_count 行的内容进行更新 |

