# 数据类型
我们目前支持大多数MYSQL定义的数据类型

| 类型        | 定义 | 最小值(有符号/无符号) | 最大值(有符号/无符号) |
| ----------- |----------|-----------------------| --------------------- |
| `TINYINT`   | 8 bit integer         |               |              |
| `SMALLINT`  | 16 bit integer        |             |          |
| `INT` | 32 bit integer       |           |     |
| `BIGINT`       | 64 bit integer       |        |  |
| `FLOAT`    | 32 bit float        |  |  |
| `DOUBLE`    | 64 bit float        |  |  |
| `VARCHAR`    | UTF-8 charset (max size 64 kb)        |  |  |
| `BINARY`    | max size 64 kb       |  |  |
| `DATE`    | YYYY-MM-DD       | `1000-01-01` |  `9999-12-31`|
| `TIMESTAMP`    | 自epoch以来的秒数        | `1970-01-01 00:00:01.000000 UTC` | `2038-01-19 03:14:07.999999 UTC`|
| `DATETIME`    | YYYY-MM-DD HH:MM:SS        | `1000-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999`|



## DATE
允许使用字符串或数字赋值

## TIMESTAMP
展示的数据受时区还有系统参数的配置影响。`0` 表示 `0000-00-00 00:00:00` 

不支持DEFAULT CURRENT_TIMESTAMP、ON UPDATE CURRENT_TIMESTAMP属性
```sql
mysql> INSERT INTO testDB.mysqltest20  values(null,"name3",1,"部门",4,99,"特长","我的地址",1,2,10,5,0.34,9.01,'字符串','binary','7985877',DEFAULT CURRENT_TIMESTAMP);
ERROR 1105 (HY000): sql parse error: syntax error. pos 145, line 1, column 128, token IDENTIFIER CURRENT_TIMESTAMP
mysql> INSERT INTO testDB.mysqltest20  values(null,"name3",1,"部门",4,99,"特长","我的地址",1,2,10,5,0.34,9.01,'字符串','binary','7985877',ON UPDATE CURRENT_TIMESTAMP);
ERROR 1105 (HY000): sql parse error: ERROR. pos 122, line 1, column 121, token ON
```

##DATETIME

允许使用字符串或数字赋值，可以使用DEFAULT和ON UPDATE属性自动初始化和更新

DEFAULT赋值不支持：
```sql
This version of MySQL doesn't yet support 'sql expr type com.alibaba.druid.sql.ast.expr.SQLDefaultExpr'
```

## DECIMAL
针对mysql的Decimal转换成java的BigDecimal进行处理，采用mysql的编码方式进行编解码。
暂不支持，问题修复中

## JSON
暂不支持，问题修复中
