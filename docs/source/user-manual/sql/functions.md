# 函数

我们目前支持多数常用的函数如下。

## SUM

```sql
SELECT y, SUM(x) FROM table group by y having SUM(x)>1
```

`SUM` 目前支持 `SUM(expression)`, `SUM(DISTINCT expression)`。

## COUNT

```sql
mysql> SELECT x, COUNT(*) FROM student group by x
mysql> SELECT x, COUNT(1) FROM student group by x
```

```sql
SELECT COUNT(DISTINCT name) FROM student

```

## MAX
```sql
SELECT MAX(column) FROM TABLE
SELECT MAX([DISTINCT] expr) FROM TABLE
```

## MIN
```sql
SELECT MIN(column) FROM TABLE
SELECT MIN([DISTINCT] expr) FROM TABLE
```

