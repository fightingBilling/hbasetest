# HBase MapReduce程序测试
	基于Hbase的MapReduce程序

## 其他运行方式测试

* MR的第三方Jar形式，测试好像不行，jar并没有进行上传...

	hadoop jar hbasetest-1.0.0.jar com.wankun.hbasetest.mapreduce.WordCountHBase -libjars /usr/lib/hbase/lib/hbase-common-0.96.1.1-cdh5.0.1.jar,/usr/lib/hbase/lib/hbase-client-0.96.1.1-cdh5.0.1.jar,/usr/lib/hbase/lib/hbase-server-0.96.1.1-cdh5.0.1.jar
	
* 直接以java命令允许，不行，虽然在本地JVM中有了CLASSPATH类库，但是在远端JVM启动的时候，CLASSPATH应该是没有携带过来

	java -cp hbasetest-1.0.0.jar:/usr/lib/hbase/*:/usr/lib/hbase/lib/*:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-yarn/*:/usr/lib/hadoop-yarn/lib/* com.wankun.hbasetest.mapreduce.WordCountHBase
	
	org.apache.hadoop.yarn.exceptions.YarnRuntimeException: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.hbase.mapreduce.TableOutputFormat not found
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster.createOutputCommitter(MRAppMaster.java:473)
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster.serviceInit(MRAppMaster.java:374)
        at org.apache.hadoop.service.AbstractService.init(AbstractService.java:163)
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster$1.run(MRAppMaster.java:1456)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:415)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1548)
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster.initAndStartAppMaster(MRAppMaster.java:1453)
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster.main(MRAppMaster.java:1386)
Caused by: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.hbase.mapreduce.TableOutputFormat not found
        at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:1895)
        at org.apache.hadoop.mapreduce.task.JobContextImpl.getOutputFormatClass(JobContextImpl.java:232)
        at org.apache.hadoop.mapreduce.v2.app.MRAppMaster.createOutputCommitter(MRAppMaster.java:469)
        ... 8 more
Caused by: java.lang.ClassNotFoundException: Class org.apache.hadoop.hbase.mapreduce.TableOutputFormat not found
        at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:1801)
        at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:1893)
        ... 10 more
        
* Fat Jar形式 ： 将程序需要的hbase包打成一个包进行运行 （未测试）

##注意事项：

1. <b>yarn-site.xml配置参数：yarn.application.classpath 中需要添加hbase的包路径</b>

	否则会看到错误
	t_1411642177244_0134_000002 exited with  exitCode: 1 due to: Exception from container-launch: org.apache.hadoop.util.Shell$ExitCodeException: 
	org.apache.hadoop.util.Shell$ExitCodeException: 
		at org.apache.hadoop.util.Shell.runCommand(Shell.java:505)


 