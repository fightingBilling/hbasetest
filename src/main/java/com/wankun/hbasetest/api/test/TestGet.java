package com.wankun.hbasetest.api.test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.RowCounter;

public class TestGet  {

	public static Log log = LogFactory.getLog(TestGet.class);

	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");

	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;
	
	public TestGet() {
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		TestGet test = new TestGet();
		test.runTest();
	}

	public void runTest()  {
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));
		
		try {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, "test_info");
			table.setAutoFlush(false);
			for (long i = 0; i < 50000; i++) {
				// 从100W中get5W数据
				try {
					String rowKey = rowKeyFormatter.format(i*(1000000/50000));
					Get get = MakeData.genGet(rowKey);
					Result result = table.get(get);
					rowCounter.countUp();
				} catch (InterruptedException e) {
					continue;
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			logmsg.setRunflag(false);
		}

		Date d2 = new Date();
		int runtime = (int) (d2.getTime() - d1.getTime()) / 1000;
		log.info("结束测试时间：" + sdf.format(d2) + " 测试运行时间：" + runtime + " 成功插入数据量：" + rowCounter.getRownum());
	}

}
