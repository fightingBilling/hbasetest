hbasetest
=========

# 程序说明
本项目测试HBase API，Thrift，Rest三种方式的数据插入和查询的性能

## 常用操作
1. 生成各种语言接口程序
thrift -gen java Hbase.thrift
2. 启动thrift服务
	bin/hbase thrift -b master -p 9090 start
	方式二：启动500个work，最小200个work
	bin/hbase-daemon.sh start thrift –threadpool -m 200 -w 500 

3. 创建测试表
	create 'test_info', 'info'
	--> 修改为使用snappy压缩
	create 'test_info', { NAME => 'info', COMPRESSION => 'snappy' } 
	--> 测试记录字段 3 -> 124 个
4. 测试运行程序
java -classpath :/root/test/hbasetest1.jar:/root/test/libs/* thrift.Test update

5.常用命令
hbase shell
truncate 'test_info'
scan 'test_info'
count 'test_info'
