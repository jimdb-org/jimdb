# 操作符

| 操作符名 | 功能描述 |
| ------- | -------------------------------- |
| [`=`](https://dev.mysql.com/doc/refman/5.7/en/assignment-operators.html#operator_assign-equal) | 赋值 (可用于 [`UPDATE`](https://dev.mysql.com/doc/refman/5.7/en/update.html) 语句的 `SET` 中 ) |
| [`/`](https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_divide) | 除法 |
| [`=`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_equal) | 相等比较 |
| [`>`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_greater-than) | 大于 |
| [`>=`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_greater-than-or-equal) | 大于或等于 |
| [`<`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_less-than) | 小于 |
| [`<=`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_less-than-or-equal) | 小于或等于 |
| [`-`](https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_minus) | 减 |
| [`%`, `MOD`](https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_mod) | 求余 |
| [`!=`, `<>`](https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#operator_not-equal) | 不等于 |
| [`+`](https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_plus) | 加 |
| [`*`](https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_times) | 乘 |

## 操作符优先级

操作符优先级显示在以下列表中，从最高优先级到最低优先级。同一行显示的操作符具有相同的优先级。

``` sql
!
*, /, %, MOD
-, +
= (comparison), >=, >, <=, <, !=
NOT
AND
OR
= (assignment), :=
```

详情参见 [这里](https://dev.mysql.com/doc/refman/5.7/en/operator-precedence.html).

## 比较方法和操作符

TODO

## 逻辑操作符

| 操作符名 | 功能描述 |
| ------- | -------------------------------- |
| [`AND`](https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_and) | 逻辑与 |
| [`NOT`, `!`](https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_not) | 逻辑非 |
| [`OR`](https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_or) | 逻辑或 |
| [`XOR`](https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_xor) | 逻辑亦或 |

详情参见 [这里](https://dev.mysql.com/doc/refman/5.7/en/group-by-handling.html).

## 赋值操作符

| 操作符名 | 功能描述 |
| ------- | -------------------------------- |
| [`=`](https://dev.mysql.com/doc/refman/5.7/en/assignment-operators.html#operator_assign-equal) | 赋值 (可用于 [`UPDATE`](https://dev.mysql.com/doc/refman/5.7/en/update.html) 语句的 `SET` 中 ) |

详情参见 [这里](https://dev.mysql.com/doc/refman/5.7/en/group-by-functional-dependence.html).
