package com.wankun.hbasetest.rest;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.RowCounter;

public class TestScan {

	public static Log log = LogFactory.getLog(TestScan.class);
	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");

	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;
	
	public TestScan() {
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		TestScan test = new TestScan();
		test.runTest();
	}
	
	public void runTest()
	{
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));
		
		try {
		    Cluster cluster = new Cluster();
		    cluster.add("h2namenode1", 9091); 
		    Client client = new Client(cluster); 
		    RemoteHTable table = new RemoteHTable(client, "test_info");
		    
			// 从100W中get2W数据
			String startkey = rowKeyFormatter.format(100000);
			String endkey = rowKeyFormatter.format(120000);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("info")).setStartRow(Bytes.toBytes(startkey)).setStopRow(Bytes.toBytes(endkey));
			ResultScanner scanner = table.getScanner(scan);
			for (Result res : scanner) {
				//	 System.out.println(res);
				rowCounter.countUp();
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
