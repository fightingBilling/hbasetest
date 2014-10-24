package com.wankun.hbasetest.load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * 对mysql中查询出的数据解析，并直接put到HBase中<br>
 * 
 * ./hbase_tools.sh com.wankun.hbasetest.load.ApiPut <br>
 * 
 * 查看插入数据：scan 'hly_temp',{LIMIT=>10}
 * 
 * @author wankun
 * @date 2014年10月13日
 * @version 1.0
 */
public class ApiPut {
	public static final String TABLENAME = "hly_temp";

	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static final String MYSQL_URL = "jdbc:mysql://192.168.39.121/test";
	public static final String MYSQL_USER = "wankun";
	public static final String MYSQL_PASSWORD = "wankun";

	public static void main(String[] args) {
		Connection dbConn = null;
		HTable htable = null;
		Statement stmt = null;
		String query = "select * from hly_temp_normal";

		try {
			dbConn = connectMysql();

			Configuration conf = HBaseConfiguration.create();
			htable = new HTable(conf, TABLENAME);
			byte[] family = Bytes.toBytes("n");

			stmt = dbConn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			// time stamp for all inserted rows
			long ts = System.currentTimeMillis();

			while (rs.next()) {
				String stationid = rs.getString("stnid");
				int month = rs.getInt("month");
				int day = rs.getInt("day");

				String rowkey = stationid + lpad(String.valueOf(month), 2, '0') + lpad(String.valueOf(day), 2, '0');
				Put p = new Put(Bytes.toBytes(rowkey));

				// get hourly data from MySQL and put into hbase
				for (int i = 5; i < 29; i++) {
					String columnI = "v" + lpad(String.valueOf(i - 4), 2, '0');
					String valueI = rs.getString(i);
					p.add(family, Bytes.toBytes(columnI), ts, Bytes.toBytes(valueI));
				}
				htable.put(p);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (dbConn != null) {
					dbConn.close();
				}
				if (htable != null) {
					htable.close();
				}

			} catch (Exception e) {
				// ignore
			}
		}
	}

	private static Connection connectMysql() throws Exception {
		String userName = MYSQL_USER;
		String password = MYSQL_PASSWORD;
		String url = MYSQL_URL;
		Class.forName(MYSQL_DRIVER).newInstance();
		Connection conn = DriverManager.getConnection(url, userName, password);

		return conn;
	}

	public static String lpad(String str, int length, char pad) {
		return String.format("%1$" + length + "s", str).replace(' ', pad);
	}
}