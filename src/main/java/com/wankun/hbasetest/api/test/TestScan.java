package com.wankun.hbasetest.api.test;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

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
			 System.out.println(res);
			rownum++;
		}
	}

}
