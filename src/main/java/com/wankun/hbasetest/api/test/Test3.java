package com.wankun.hbasetest.api.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.RowCounter;

/**
 * java -classpath :/home/wank/test/hbasetest1.jar:/home/wank/test/libs/* api.test.Test3
 * 
 * 插入方式	autoflash	flush commit	插入记录数	插入时间	插入效率
1000条批量	默认	开启	5000000	75	66667 
1000条批量	默认	关闭	5000000	76	65789 
1000条批量	FALSE	开启	5000000	74	67568 
1000条批量	FALSE	关闭	5000000	68	73529 
单记录	默认	开启	50000	56	893 
单记录	默认	关闭	50000	55	909 
单记录	FALSE	开启	50000	55	909 
单记录	FALSE	关闭	5000000	66	75758 

 * @author wankun
 *
 */
public class Test3 {

	public static Log log = LogFactory.getLog(Test3.class);

	private static long l = 308948191001l;

	// 生产数据的现场
	private MakeData md;
	private Thread mdth;
	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;

	public Test3() {
		md = new MakeData();
		mdth = new Thread(md);
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		Test3 test = new Test3();
		test.runTest();
	}

	public void runTest() {
		mdth.start();
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));

		Configuration conf = HBaseConfiguration.create();
		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = HConnectionManager.createConnection(conf);
			table = connection.getTable("test_info");
			table.setAutoFlush(false, false);
			List<Put> puts;
			for (long i = 0; i < 5000; i++) {
				puts = new ArrayList<Put>();
				for (int j = 0; j < 1000; j++) {
					Put put = genPut();
					puts.add(put);
					rowCounter.countUp();
				}
				table.put(puts);
				table.flushCommits();
			}

			//			for (long i = 0; i < 50000; i++) {
			//				Put put = genPut();
			//				rowCounter.countUp();
			//				table.put(put);
			//				table.flushCommits();
			//			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			logmsg.setRunflag(false);
			md.setRunflag(false);
		}

		Date d2 = new Date();
		int runtime = (int) (d2.getTime() - d1.getTime()) / 1000;
		log.info("结束测试时间：" + sdf.format(d2) + " 测试运行时间：" + runtime + " 成功插入数据量：" + rowCounter.getRownum());
	}

	public static synchronized Put genPut() {
		l = l + 1;
		StringBuffer rowkey = new StringBuffer();
		rowkey.append("0000000000192013012400").append(l).append(",1,1");
		Put put = new Put(Bytes.toBytes(rowkey.toString()));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("receipt_content"), Bytes.toBytes("0000000000192013012400308948191001,111118374975,1,1,0519,20130124155902,JVB123"));
		return put;
	}

}
