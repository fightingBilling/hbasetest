package com.wankun.hbasetest.rest;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
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
	public void test(RemoteHTable table) throws Exception {
		// 从100W中get2W数据
		String startkey = rowKeyFormatter.format(100000);
		String endkey = rowKeyFormatter.format(120000);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info")).setStartRow(Bytes.toBytes(startkey)).setStopRow(Bytes.toBytes(endkey));
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			// System.out.println(res);
			rownum++;
		}

	}
}
