# HBase 数据导入
	
	参考资料：http://www.csdn.net/article/2014-01-07/2818046
	github:https://github.com/uprush/hac-book
	
	HBase数据导入有三种形式，API数据打入，Bolk Load工具，MapReduce Job
	
## API 方式数据导入

1. MySQL 数据准备

1.1. 数据文件:(美国国家海洋和大气管理局 1981-2010气候平均值)
	http://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/products/hourly/hly-temp-normal.txt
	
1.2. 创建Mysql数据表
```
	create table hly_temp_normal (
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	stnid CHAR(11),
	month TINYINT,
	day TINYINT,
	value1 VARCHAR(5),
	value2 VARCHAR(5),
	value3 VARCHAR(5),
	value4 VARCHAR(5),
	value5 VARCHAR(5),
	value6 VARCHAR(5),
	value7 VARCHAR(5),
	value8 VARCHAR(5),
	value9 VARCHAR(5),
	value10 VARCHAR(5),
	value11 VARCHAR(5),
	value12 VARCHAR(5),
	value13 VARCHAR(5),
	value14 VARCHAR(5),
	value15 VARCHAR(5),
	value16 VARCHAR(5),
	value17 VARCHAR(5),
	value18 VARCHAR(5),
	value19 VARCHAR(5),
	value20 VARCHAR(5),
	value21 VARCHAR(5),
	value22 VARCHAR(5),
	value23 VARCHAR(5),
	value24 VARCHAR(5)
	);
```	 

1.3. 数据导入脚本
	https://github.com/uprush/hac-book/blob/master/2-data-migration/script/insert_hly.py

1.4. 在HBase中创建 hly_temp 表 
	
	create 'hly_temp', {NAME => 'n', VERSIONS => 1} 

2. 运行
	通过工具脚本运行  ./hbase_tools.sh com.wankun.hbasetest.load.ApiPut
	
	
## bulk load 工具从TSV文件中导入

1. 数据准备

1.1. 数据文件:(美国国家海洋和大气管理局 1981-2010气候平均值)
	http://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/products/hourly/hly-temp-10pctl.txt
	
1.2. 转换文件为TSV格式，并传到HDFS上
	
	python to_tsv_hly.py -f hly-temp-10pctl.txt -t hly-temp-10pctl.tsv
	hadoop dfs -mkdir -p /tmp/wankun/tmp2
	hadoop dfs -put hly-temp-10pctl.tsv /tmp/wankun/tmp2
	
1.3. HBase 建表

	create 'hly_temp', {NAME => 't', VERSIONS => 1},{NAME=>'n',VERSIONS=>1}
#	create 'hly_temp', {NAME => 't', VERSIONS => 1}
#	alter 'hly_temp' ,{NAME=>'n',VERSIONS=>1}

2. 使用ImportTsv，生成HFile
	
	hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  -Dimporttsv.columns=HBASE_ROW_KEY,t:v01,t:v02,t:v03,t:v04,t:v05,t:v06,t:v07,t:v08,t:v09,t:v10,t:v11,t:v12,t:v13,t:v14,t:v15,t:v16,t:v17,t:v18,t:v19,t:v20,t:v21,t:v22,t:v23,t:v24 -Dimporttsv.bulk.output=/tmp/wankun/tmp3 hly_temp /tmp/wankun/tmp2	
	
	执行结果会自动创建目录：/tmp/wankun/tmp3，并生成表目录 t,以及表的HFile文件
 
3. CompleteBulkLoad 实用工具可以将产生的存储文件移动到HBase表 
 
* 方式一
	
	hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/wankun/tmp3 hly_temp
		
* 方式二
	
	HADOOP_CLASSPATH=`hbase classpath` hadoop jar /usr/lib/hbase/hbase-server.jar completebulkload /tmp/wankun/tmp3 hly_temp
	
	
4. 使用ImportTsv，直接导入数据到HBase
	为步骤2 ，去除参数  -Dimporttsv.bulk.output即可

* 方式一
	
	hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,t:v01,t:v02,t:v03,t:v04,t:v05,t:v06,t:v07,t:v08,t:v09,t:v10,t:v11,t:v12,t:v13,t:v14,t:v15,t:v16,t:v17,t:v18,t:v19,t:v20,t:v21,t:v22,t:v23,t:v24 hly_temp /tmp/wankun/tmp2/hly-temp-10pctl.tsv
	
	工具默认以tab键分割字段，也可以通过-Dimporttsv.separator=","来指定分隔符

* 方式二

	HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` ${HADOOP_HOME}/bin/hadoop jar ${HBASE_HOME}/hbase-VERSION.jar importtsv -Dimporttsv.columns=HBASE_ROW_KEY,d:c1,d:c2 -Dimporttsv.bulk.output=hdfs://storefileoutput  datatsv hdfs://inputfile
	
## HBase 预分区

* 命令创建预分区

`
	hbase org.apache.hadoop.hbase.util.RegionSplitter -c 10 -f n hly_temp HexStringSplit
	-c : 分的区数
	-f : 列族
	table_name : 表名
	SPLITALGORITHM ： 分区的算法，
			HexStringSplit： 
			UniformSplit 
`
	  
* 脚本创建预分区

`	
	# Optionally pre-split the table into NUMREGIONS, using SPLITALGO ("HexStringSplit", "UniformSplit" or classname)
	create 't1', 'f1', {NUMREGIONS => 15, SPLITALGO => 'HexStringSplit'}
`
	create 't', {NAME => 'f', VERSIONS => 1, COMPRESSION => 'SNAPPY'}, {SPLITS => ['10','20','30']}
		此时建立4个分区，分区的start key 分别为 null,10,20,30
	
	
	