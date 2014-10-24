#/bin/bash
bin=`dirname $0`
bin=`cd $bin;pwd`
HBASE_CONF=/etc/hbase/conf
HADOOP_HOME=/usr/lib/hadoop
HDFS_HOME=/usr/lib/hadoop-hdfs
YARN_HOME=/usr/lib/hadoop-yarn
MAPREDUCE_HOME=/usr/lib/hadoop-mapreduce
HBASE_LIB=/usr/lib/hbase/lib

cp=.:./*:$HBASE_CONF/*:$HBASE_LIB/*:$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$HDFS_HOME/*:$HDFS_HOME/lib/*:$YARN_HOME/*:$YARN_HOME/lib/*:$MAPREDUCE_HOME/*:$MAPREDUCE_HOME/lib/*
#for jar in $bin/*.jar
#do
#  cp=$cp:$jar
#done
#for jar in $bin/lib/*.jar
#do
#  cp=$cp:$jar
#done
#for jar in $HBASE_HOME/lib/*.jar
#do
#  cp=$cp:$jar
#done
#echo $cp
$JAVA_HOME/bin/java -classpath $cp $1
