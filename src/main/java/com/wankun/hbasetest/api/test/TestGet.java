package com.wankun.hbasetest.api.test;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.SimpleData;

/**
 * 
 * 字段的多少影响不大，平均get数就600左右
 * 
 * @author wankun
 * @date 2014年10月10日
 * @version 1.0
 */
public class TestGet extends HBaseTest {

	public static void main(String[] args) {
		TestGet test = new TestGet();
		test.runTest();
	}

	@Override
	public void init() {
	}

	@Override
	public void test(HTable table) throws Exception {
		getLong(table);
	}

	/**
	 * 平均处理效率：667
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void getShort(HTable table) throws Exception {
		table.setAutoFlush(false);
		for (long i = 0; i < 50000; i++) {
			// 从100W中get5W数据
			String rowkey = rowKeyFormatter.format(i * (1000000 / 50000));
			Get get = SimpleData.genGet(rowkey);
			Result result = table.get(get);
			rownum++;
		}
	}

	/**
	 * 平均处理效率：534
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void getLong(HTable table) throws Exception {
		table.setAutoFlush(false);
		for (long i = 0; i < 50000; i++) {
			// 从100W中get5W数据
			String rowKey = rowKeyFormatter.format(i * (1000000 / 50000));
			Get get = MakeData.genGet(rowKey);
			Result result = table.get(get);
			rownum++;
		}
	}
}
