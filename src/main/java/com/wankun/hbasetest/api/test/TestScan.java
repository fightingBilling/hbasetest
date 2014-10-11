package com.wankun.hbasetest.api.test;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <pre>
 * java -classpath hbasetest-1.0.0.jar:/usr/lib/hbase/*:/usr/lib/hbase/lib/*:/usr/lib/hadoop/*:/usr/lib/hadoop-hdfs/* com.wankun.hbasetest.api.test.TestScan
 * </pre>
 * 
 * 测试平均处理效率：
 * 		hbase master：6311
 * 		client eclipse:1531
 * 
 * @author wankun
 * @date 2014年10月10日
 * @version 1.0
 */
public class TestScan extends HBaseTest {

	public static void main(String[] args) {
		TestScan test = new TestScan();
		test.runTest();
	}

	@Override
	public void init() {
	}

	@Override
	public void test(HTable table) throws Exception {
		// 从100W中get20W数据
		String startkey = rowKeyFormatter.format(100000);
		String endkey = rowKeyFormatter.format(300000);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info")).setStartRow(Bytes.toBytes(startkey)).setStopRow(Bytes.toBytes(endkey));
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			rownum++;
		}
	}

}
