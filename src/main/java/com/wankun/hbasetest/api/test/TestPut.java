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
import org.apache.hadoop.hbase.util.Bytes;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.RowCounter;

/**
 * 
 * @author wankun
 *
 */
public class TestPut {
	public static Log log = LogFactory.getLog(TestPut.class);
	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");
	// 生产数据的现场
	private MakeData md;
	private Thread mdth;
	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;

	public TestPut() {
		md = new MakeData();
		mdth = new Thread(md);
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		TestPut test = new TestPut();
		test.runTest();
	}

	public void runTest() {
		mdth.start();
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));

		try {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, "test_info");
			table.setAutoFlush(false);
			for (int i = 0; i < 1000000; i++) {
				try {
					String rowKey = rowKeyFormatter.format(i);
					Put put = genPut4(rowKey);
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
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col06"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col07"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col08"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col09"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col10"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col11"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col12"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col13"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col14"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col15"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}
	
	public Put genPut3(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col06"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col07"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col08"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col09"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col10"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}
	
	public Put genPut4(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col01"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col02"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col03"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col04"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("col05"), Bytes.toBytes("测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据测试数据"));
		return put;
	}

}
