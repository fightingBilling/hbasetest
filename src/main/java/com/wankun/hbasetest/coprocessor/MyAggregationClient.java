package com.wankun.hbasetest.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class MyAggregationClient {

	public static void main(String[] args) throws Throwable {
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "dev01.dc.ztgame.com:2181");
		// 提高RPC通信时长
		customConf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		customConf.setLong("hbase.client.scanner.caching", 1000);
		Configuration conf = HBaseConfiguration.create(customConf);

		String tableName = "hly_temp";

		// 为指定表添加coprocessor
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
		htd.removeCoprocessor(AggregateImplementation.class.getName());
		htd.addCoprocessor(AggregateImplementation.class.getName(), new Path(
				"hdfs://mycluster/tmp/wankun/hbasetest-1.0.0.jar"), 10, null);
		// htd.addCoprocessor(coprocessClassName);
//		htd.removeCoprocessor(AggregateImplementation.class.getName());
		admin.modifyTable(tableName, htd);
		admin.enableTable(tableName);
		admin.close();

		// AggregationClient aggregationClient = new AggregationClient(conf);
		// HTable htable = new HTable(conf, tableName);
		// Scan scan = new Scan();
		// // 指定扫描列族，唯一值
		// scan.addFamily(Bytes.toBytes("n"));
		// // new LongColumnInterpreter()
		// long rowCount = aggregationClient.rowCount(htable, null, scan);
		// System.out.println("row count is " + rowCount);

		AggregationClient client = new AggregationClient(conf);
		Scan scan = new Scan();
		Long count = client.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);

		System.out.println(count);

	}
}
