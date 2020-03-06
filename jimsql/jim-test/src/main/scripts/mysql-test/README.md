# mysqltest自动化测试框架
简介：mysqltest自动化测试框架，在mysql-test基础上进行改造，去除一些依赖，简化框架，框架目前主要包含bin、lib、r、t、var 目录，2个perl脚本mysql-test-run.pl和mysql-test-run-startserver.pl脚本
# 各个模块功能介绍
1. bin
bin下面包括执行的mysqltest二进制文件，包括mac和linux2个版本
2. lib
lib下面包括 perl脚本依赖的各个模块
3. r 
测试用例默认的目录，测试用例格式：[名称.test]
4. t
测试结果默认的目录，测试结果文件名称和测试用例一样后缀不一样，后缀为 [名称.result],框架默认会找结果目录下同名的文件
5、var
运行产生临时文件my.cnf,生成日志的文件

# mysql-test-run.pl 介绍
mysql-test-run.pl是perl脚本编写的，执行mysqltest运行mysql的测试用例，包括用例运行并进行结果比对。
1. 脚本的常用参数介绍
    - 脚本执行命令： `perl mysql-test-run.pl [options] [test_name]`
    - 执行远程服务的命令
    `
    perl mysql-test-run.pl --extern host=127.0.0.1 --extern port=3360 --extern user=root --extern password='root' 1st
    `
    ---
    
    - --extern option=value : extern 参数后面可以用host、port、user等指定测试的服务
    - --record 此参数默认会在r文件夹下面生成 xxx.result 结果文件
    - --verbose 日志开关，默认一些详细日志关闭，加上后会打开详细日志
    - --xml-report=file_name 会生成xml的报告文件
2. 不加参数执行默认会启动服务并执行测试用例
`perl mysql-test-run.pl 用例前缀`
3. 详细参数参考：
https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_MYSQL_TEST_RUN_PL.html
https://s0dev0mysql0com.icopy.site/doc/dev/mysql-server/8.0.12/PAGE_MYSQL_TEST_VARIABLES.html
# 如何编写测试用例
1. 编写测试用例规范
- case的命名，尽量规范，如可以按照以下格式进行命名:
主要功能_次要功能_编号.test
- 编写case时，尽可能加注释，标注清楚该case的测试项及测试点等信息；
- case中表和字段的命名，表名尽可能使用t1，t2，…,字段名尽可能用c1,c2,…,这样方便查阅，可在一定程度上提高可读性。
- 在写case时，尽可能避免出现这种容易随着时间、运行环境的变化而产生不同的数据的sql，不要出现这种sql执行结果变化的量。
- case文件中包含的测试项要适中，不要过多或过少，因为在测试时可能会影响效率，而且不便管理；
- 在多个case中，应尽量避免出现重复的测试点，测试项。
- 尽可能避免每行超过80个字符
- 使用#开头，作为注释
- 缩进使用空格，避免使用tab








