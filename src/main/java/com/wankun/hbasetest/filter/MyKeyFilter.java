package com.wankun.hbasetest.filter;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MyKeyFilter {

	private static String tableName = "hly_temp";

	public static void main(String[] args) throws IOException {
		Configuration conf =HBaseConfiguration.create(); 
		HTable table = new HTable(conf,tableName);
		table.setScannerCaching(100);
		table.setAutoFlush(false);
		Scan s  = new Scan();
		s.setFilter(new FirstKeyOnlyFilter());
		s.setBatch(100);
		ResultScanner rs = table.getScanner(s);
		long startTime = System.currentTimeMillis();
		System.out.println(startTime);
		for (Result rr = rs.next(); rr != null; rr = rs.next()) {
			Get g = new Get(rr.getRow());
			g.setFilter(new ColumnCountGetFilter(100));
			System.out.println(Bytes.toString(rr.getRow()));
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> valueMap = table.get(g).getNoVersionMap();
			for (byte[] family:valueMap.keySet()) {
				for (byte[] val: valueMap.get(family).values()) {
					System.out.println(Bytes.toString(val));
				}
			}
		}
		System.out.println((System.currentTimeMillis()- startTime) + "ms");
	}
}
