package com.wankun.hbasetest.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.RowCounter;

public class TestGet {

	public static Log log = LogFactory.getLog(TestGet.class);
	private static final String CHARSET = "UTF-8";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DecimalFormat rowKeyFormatter = new DecimalFormat("0000000000000");
	private final AbstractHBaseThriftService client;

	private List<String> columns = null;

	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;

	private TestGet(String host, int port) {
		client = new HBaseThriftClient(host, port);
		try {
			client.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}

	public TestGet() {
		this("h2namenode1", 9090);
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
		initColumns();
	}

	public static void main(String[] args) {
		TestGet test = new TestGet();
		test.runTest();
	}

	public void runTest() {
		logmsgth.start();
		Date d1 = new Date();
		log.info("开始测试时间：" + sdf.format(d1));

		Map<String, String> attributes = new HashMap<String, String>(0);
		List<String> rows;
		for (long i = 0; i < 50000; i++) {
			// 从100W中get5W数据
			try {
				String rowKey = rowKeyFormatter.format(i * (1000000 / 50000));
				rows = new ArrayList<String>();
				rows.add(rowKey);
				List<TRowResult> results = client.getRowsWithColumns("test_info", rows, columns, attributes);
				rowCounter.countUp();
			} catch (TException e) {
				e.printStackTrace();
			}
		}

		logmsg.setRunflag(false);
		Date d2 = new Date();
		int runtime = (int) (d2.getTime() - d1.getTime()) / 1000;
		log.info("结束测试时间：" + sdf.format(d2) + " 测试运行时间：" + runtime + " 成功插入数据量：" + rowCounter.getRownum());
	}

	static ByteBuffer wrap(String value) {
		ByteBuffer bb = null;
		try {
			bb = ByteBuffer.wrap(value.getBytes(CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return bb;
	}

	public void initColumns() {
		columns = new ArrayList<String>();
		columns.add("info:source_type");
		columns.add("info:record_type");
		columns.add("info:ni_pdp");
		columns.add("info:msisdn");
		columns.add("info:imsi_number");
		columns.add("info:sgsn");
		columns.add("info:msnc");
		columns.add("info:lac");
		columns.add("info:ra");
		columns.add("info:cell_id");
		columns.add("info:charging_id");
		columns.add("info:ggsn");
		columns.add("info:apnni");
		columns.add("info:apnoi");
		columns.add("info:pdp_type");
		columns.add("info:spa");
		columns.add("info:sgsn_change");
		columns.add("info:cause_close");
		columns.add("info:result");
		columns.add("info:home_area_code");
		columns.add("info:visit_area_code");
		columns.add("info:user_type");
		columns.add("info:fee_type");
		columns.add("info:roam_type");
		columns.add("info:service_type");
		columns.add("info:start_date");
		columns.add("info:start_time");
		columns.add("info:call_duration");
		columns.add("info:tariff1");
		columns.add("info:data_up1");
		columns.add("info:data_down1");
		columns.add("info:duration1");
		columns.add("info:tariff2");
		columns.add("info:data_up2");
		columns.add("info:data_down2");
		columns.add("info:duration2");
		columns.add("info:tariff3");
		columns.add("info:data_up3");
		columns.add("info:data_down3");
		columns.add("info:duration3");
		columns.add("info:tariff4");
		columns.add("info:data_up4");
		columns.add("info:data_down4");
		columns.add("info:duration4");
		columns.add("info:tariff5");
		columns.add("info:data_up5");
		columns.add("info:data_down5");
		columns.add("info:duration5");
		columns.add("info:tariff6");
		columns.add("info:data_up6");
		columns.add("info:data_down6");
		columns.add("info:duration6");
		columns.add("info:cfee");
		columns.add("info:fee1");
		columns.add("info:fee2");
		columns.add("info:fee3");
		columns.add("info:total_fee");
		columns.add("info:deal_time");
		columns.add("info:file_no");
		columns.add("info:error_code");
		columns.add("info:cust_id");
		columns.add("info:user_id");
		columns.add("info:a_brand_id");
		columns.add("info:pay_mode");
		columns.add("info:a_serv_type");
		columns.add("info:chan_no");
		columns.add("info:office_code");
		columns.add("info:free_code");
		columns.add("info:billing_cycle");
		columns.add("info:bill_start_day");
		columns.add("info:revision");
		columns.add("info:cdr_day");
		columns.add("info:file_pp_name");
		columns.add("info:visit_county");
		columns.add("info:deduct_remark");
		columns.add("info:deduct_remark2");
		columns.add("info:imei");
		columns.add("info:recordextension");
		columns.add("info:billinfo");
		columns.add("info:rcdseqnum");
		columns.add("info:apnni_groupid");
		columns.add("info:free_item1");
		columns.add("info:free1");
		columns.add("info:product_id1");
		columns.add("info:instance_id1");
		columns.add("info:accu_user_id1");
		columns.add("info:free_item2");
		columns.add("info:free2");
		columns.add("info:product_id2");
		columns.add("info:instance_id2");
		columns.add("info:accu_user_id2");
		columns.add("info:free_item3");
		columns.add("info:free3");
		columns.add("info:product_id3");
		columns.add("info:instance_id3");
		columns.add("info:accu_user_id3");
		columns.add("info:free_item4");
		columns.add("info:free4");
		columns.add("info:product_id4");
		columns.add("info:instance_id4");
		columns.add("info:accu_user_id4");
		columns.add("info:ratemethod");
		columns.add("info:free_code1");
		columns.add("info:free_fee1");
		columns.add("info:rate_item_id1");
		columns.add("info:free_code2");
		columns.add("info:free_fee2");
		columns.add("info:rate_item_id2");
		columns.add("info:free_code3");
		columns.add("info:free_fee3");
		columns.add("info:rate_item_id3");
		columns.add("info:free_code4");
		columns.add("info:free_fee4");
		columns.add("info:rate_item_id4");
		columns.add("info:base_tpremark");
		columns.add("info:tpremark");
		columns.add("info:rate_file_name");
		columns.add("info:rate_time");
		columns.add("info:acc_file_name");
		columns.add("info:acc_time");
		columns.add("info:dis_file_name");
		columns.add("info:dis_time");
		columns.add("info:busi_domain");
		columns.add("info:bill_end_day");

	}

}
