package com.wankun.hbasetest.api.test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import com.google.common.base.Stopwatch;

public abstract class HBaseTest {

	public static Log logger = LogFactory.getLog(HBaseTest.class);

	public static int rownum = 0;
	public DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");

	public abstract void init();

	public abstract void test(HTable table) throws Exception;

	public void runTest() {
		init();
		new Timer(true).schedule(new TimerTask() {
			long lastcount = 0;

			@Override
			public void run() {
				logger.info("LogMessage \t[allrow] : " + rownum + "\t\t  [rate]" + (rownum - lastcount));
				lastcount = rownum;
			}

		}, 0, 1000);

		logger.info("开始测试时间：");
		Stopwatch watch = new Stopwatch();
		watch.start();

		HTable table = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			table = new HTable(conf, "test_info");
			test(table);
		} catch (Exception e) {
			logger.error("测试运行出错", e);
		} finally {
			if (table != null)
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		watch.stop();
		logger.info("结束测试");
		logger.info("测试运行时间:" + watch.elapsedMillis() + " 成功操作数据量：" + rownum + "  平均处理效率：" + rownum * 1000.0
				/ watch.elapsedMillis());
	}

}
