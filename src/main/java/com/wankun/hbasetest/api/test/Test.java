package com.wankun.hbasetest.api.test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.RowCounter;

public class Test  {

	public static Log log = LogFactory.getLog(Test.class);

	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");

	// 生产数据的现场
	private MakeData md;
	private Thread mdth;
	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;
	
	public Test() {
		md = new MakeData();
		mdth = new Thread(md);
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		Test test = new Test();
		test.runTest();
	}

	public void runTest()  {
		mdth.start();
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));
		
		try {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, "test_info");
			table.setAutoFlush(false);
			for (long i = 0; i < 1000000; i++) {
				// 如果运行出错，直接提过，这样在结果集中rowcount会小于1000000
				try {
					String rowKey = rowKeyFormatter.format(i);
					Put put = MakeData.genPut(md,rowKey);
					table.put(put);
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
			md.setRunflag(false);
		}

		Date d2 = new Date();
		int runtime = (int) (d2.getTime() - d1.getTime()) / 1000;
		log.info("结束测试时间：" + sdf.format(d2) + " 测试运行时间：" + runtime + " 成功插入数据量：" + rowCounter.getRownum());
	}

}
