package com.wankun.hbasetest.api.test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Stopwatch;

/**
 * 
 * @author wankun
 * 
 */
public class TestPut {
	public static Log log = LogFactory.getLog(TestPut.class);

	public static final int ALLROWS = 100000;
	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");

	public static void main(String[] args) {
		TestPut test = new TestPut();
		test.runTest();
	}

	public static int rownum = 0;

	public void runTest() {
		new Timer(true).schedule(new TimerTask() {
			long lastcount = 0;

			@Override
			public void run() {
				log.info("LogMessage \t[allrow] : " + rownum + "\t\t  [rate]" + (rownum - lastcount));
				lastcount = rownum;
			}

		}, 0, 1000);

		log.info("开始测试时间：");
		Stopwatch watch= new Stopwatch();
		watch.start();

		HTable table = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			table = new HTable(conf, "test_info");
			table.setAutoFlush(false);
			for (int i = 0; i < ALLROWS; i++) {
				String rowKey = rowKeyFormatter.format(i);
				Put put = genPut4(rowKey);
				table.put(put);
				rownum++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (table != null)
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		watch.stop();
		log.info("结束测试");
		log.info("测试运行时间:" + watch.elapsedMillis() + " 成功操作数据量：" + rownum+"  平均处理效率："+rownum*1000.0/watch.elapsedMillis());
	}

	public Put genPut1(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col06"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col07"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col08"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col09"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col10"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col11"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col12"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col13"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col14"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col15"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col16"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col17"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col18"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col19"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col20"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col21"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col22"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col23"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col24"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col25"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col26"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col27"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col28"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col29"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col30"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}

	public Put genPut2(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col06"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col07"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col08"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col09"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col10"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col11"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col12"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col13"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col14"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col15"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}

	public Put genPut3(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col06"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col07"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col08"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col09"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col10"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}

	public Put genPut4(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"),
				Bytes.toBytes("col01"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"),
				Bytes.toBytes("col02"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"),
				Bytes.toBytes("col03"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"),
				Bytes.toBytes("col04"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"),
				Bytes.toBytes("col05"),
				Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}

}
