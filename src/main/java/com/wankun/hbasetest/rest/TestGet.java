package com.wankun.hbasetest.rest;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;

import com.wankun.hbasetest.util.MakeData;

public class TestGet extends HBaseTest {

	public static void main(String[] args) {
		TestGet test = new TestGet();
		test.runTest();
	}

	@Override
	public void init() {
	}

	@Override
	public void test(RemoteHTable table) throws Exception {
		// table.setAutoFlush(false);
		for (long i = 0; i < 10000; i++) {
			// 从100W中get1W数据
			String rowKey = rowKeyFormatter.format(i * (1000000 / 10000));
			Get get = MakeData.genGet(rowKey);
			Result result = table.get(get);
			rownum++;
		}

	}
}
