package com.wankun.hbasetest.util;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleData {

	public static Put genPut(long l) {
		StringBuffer rowkey = new StringBuffer();
		rowkey.append("0000000000192013012400").append(l).append(",1,1");
		Put put = new Put(Bytes.toBytes(rowkey.toString()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("receipt_content"),
				Bytes.toBytes("0000000000192013012400308948191001,111118374975,1,1,0519,20130124155902,JVB123"));
		return put;
	}
	
	public static Get genGet(String rowkey) {
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("receipt_content"));
		return get;
	}
}
