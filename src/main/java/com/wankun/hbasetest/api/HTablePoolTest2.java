package com.wankun.hbasetest.api;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * http://www.myexception.cn/database/1300285.html
 */
public class HTablePoolTest2 {

	protected static String TEST_TABLE_NAME = "testtable";

	protected static String ROW1_STR = "row1";
	protected static String COLFAM1_STR = "colfam1";
	protected static String QUAL1_STR = "qual1";

	private final static byte[] ROW1 = Bytes.toBytes(ROW1_STR);
	private final static byte[] COLFAM1 = Bytes.toBytes(COLFAM1_STR);
	private final static byte[] QUAL1 = Bytes.toBytes(QUAL1_STR);

	private static HTablePool pool;

	@BeforeClass
	public static void runBeforeClass() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		// 默认使用PoolType.Reusable
		pool = new HTablePool(conf, 10);
		// 初始化填充pool
		HTableInterface[] tables = new HTableInterface[10];
		for (int n = 0; n < 10; n++) {
			tables[n] = pool.getTable(TEST_TABLE_NAME);
		}
		// close后,PooledTable就放回了pool
		for (HTableInterface table : tables) {
			table.close();
		}
	}

	@Test
	public void testHTablePool() throws IOException, InterruptedException, ExecutionException {
		Callable<Result> callable = new Callable<Result>() {
			public Result call() throws Exception {
				return get();
			}
		};
		FutureTask<Result> task1 = new FutureTask<Result>(callable);
		FutureTask<Result> task2 = new FutureTask<Result>(callable);
		Thread thread1 = new Thread(task1, "THREAD-1");
		thread1.start();
		Thread thread2 = new Thread(task2, "THREAD-2");
		thread2.start();
		Result result1 = task1.get();
		System.out.println(Bytes.toString(result1.getValue(COLFAM1, QUAL1)));
		Result result2 = task2.get();
		System.out.println(Bytes.toString(result2.getValue(COLFAM1, QUAL1)));
	}

	private Result get() {
		HTableInterface table = pool.getTable(TEST_TABLE_NAME);
		Get get = new Get(ROW1);
		try {
			Result result = table.get(get);
			return result;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

