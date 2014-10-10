package com.wankun.hbasetest.rest;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;

import com.wankun.hbasetest.util.MakeData;

public class Test extends HBaseTest {

	public static void main(String[] args) {
		Test test = new Test();
		test.runTest();
	}

	public MakeData md = new MakeData();

	@Override
	public void init() {
		md.start();
	}

	@Override
	public void test(RemoteHTable table) throws Exception {
		// table.setAutoFlush(false);
		for (long i = 0; i < 10000; i++) {
			// 如果运行出错，直接提过，这样在结果集中rowcount会小于10000
			String rowKey = rowKeyFormatter.format(i);
			Put put = MakeData.genPut(md, rowKey);
			table.put(put);
			rownum++;
		}

	}
}
