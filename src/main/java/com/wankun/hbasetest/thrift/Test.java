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
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Stopwatch;
import com.wankun.hbasetest.util.GprsBean;
import com.wankun.hbasetest.util.MakeData;

public class Test {

	public static Log logger = LogFactory.getLog(Test.class);
	private static final String CHARSET = "UTF-8";
	private final AbstractHBaseThriftService client;

	private Test(String host, int port) {
		client = new HBaseThriftClient(host, port);
		try {
			client.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}

	public Test() {
		this("h2namenode1", 9090);
	}

	public static void main(String[] args) {
		String op = "";
		if (args.length != 1) {
			System.out.println("请带操作参数!");
			System.exit(0);
		} else {
			op = args[0];
		}

		Test test = new Test();
		test.runTest(op);

	}

	public MakeData md = new MakeData();
	public static int rownum = 0;

	public void runTest(String op) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();

		new Timer(true).schedule(new TimerTask() {
			long lastcount = 0;

			@Override
			public void run() {
				logger.info("LogMessage \t[allrow] : " + rownum + "\t\t  [rate]" + (rownum - lastcount));
				lastcount = rownum;
			}

		}, 0, 1000);
		logger.info("开始测试时间：" + sdf.format(d1));
		Stopwatch watch = new Stopwatch();
		watch.start();

		try {
			if ("update".equals(op))
				caseForUpdate(); // insert or update rows/cells
			if ("deletecell".equals(op))
				caseForDeleteCells(); // delete cells
			if ("deleterow".equals(op))
				caseForDeleteRow(); // insert or update rows/cells
			if ("scan".equals(op))
				caseForScan(); // scan rows
			if ("get".equals(op))
				caseForGet(); // get rows
		} catch (TException e) {
			e.printStackTrace();
		}

		watch.stop();
		logger.info("结束测试");
		logger.info("测试运行时间:" + watch.elapsedMillis() + " 成功操作数据量：" + rownum + "  平均处理效率：" + rownum * 1000.0
				/ watch.elapsedMillis());

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

	static DecimalFormat rowKeyFormatter = new DecimalFormat("0000000000000");

	public void caseForUpdate() {
		boolean writeToWal = false;
		Map<String, String> attributes = new HashMap<String, String>(0);
		String table = setTable();
		// put kv pairs
		for (long i = 0; i < 100000; i++) {
			// 在插入数据的时候捕获异常，如果数据插入出错，rownum会明显小于100000
			try {
				rownum++;
				String rowKey = rowKeyFormatter.format(i);
				Map<String, String> fieldNameValues = genKVMap();
				client.update(table, rowKey, writeToWal, fieldNameValues, attributes);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	public void caseForDeleteCells() throws TException {
		boolean writeToWal = false;
		Map<String, String> attributes = new HashMap<String, String>(0);
		String table = setTable();
		// put kv pairs
		for (long i = 5; i < 10; i++) {
			String rowKey = rowKeyFormatter.format(i);
			List<String> columns = new ArrayList<String>(0);
			columns.add("info:birthday");
			client.deleteCells(table, rowKey, writeToWal, columns, attributes);
		}
	}

	private String setTable() {
		String table = "test_info";
		return table;
	}

	public void caseForDeleteRow() throws TException {
		Map<String, String> attributes = new HashMap<String, String>(0);
		String table = setTable();
		// delete rows
		for (long i = 5; i < 10; i++) {
			String rowKey = rowKeyFormatter.format(i);
			client.deleteRow(table, rowKey, attributes);
		}
	}

	public void caseForScan() throws TException {
		Map<String, String> attributes = new HashMap<String, String>(0);
		String table = setTable();
		String startRow = "0005";
		String stopRow = "0015";
		List<String> columns = new ArrayList<String>(0);
		columns.add("info:birthday");
		int id = client.scannerOpen(table, startRow, stopRow, columns, attributes);
		int nbRows = 2;
		List<TRowResult> results = client.scannerGetList(id, nbRows);
		while (results != null && !results.isEmpty()) {
			for (TRowResult result : results) {
				client.iterateResults(result);
			}
			results = client.scannerGetList(id, nbRows);
		}
		client.scannerClose(id);
	}

	public void caseForGet() throws TException {
		Map<String, String> attributes = new HashMap<String, String>(0);
		String table = setTable();
		List<String> rows = new ArrayList<String>(0);
		rows.add("0009");
		rows.add("0098");
		rows.add("0999");
		List<String> columns = new ArrayList<String>(0);
		columns.add("info:birthday");
		columns.add("info:gender");
		List<TRowResult> results = client.getRowsWithColumns(table, rows, columns, attributes);
		for (TRowResult result : results) {
			client.iterateResults(result);
		}
	}

	public Map<String, String> genKVMap() throws InterruptedException {
		Map<String, String> fieldNameValues = new HashMap<String, String>();
		GprsBean gb = md.getQueue().take();
		fieldNameValues.put("info:source_type", gb.getSOURCE_TYPE() + "");
		fieldNameValues.put("info:record_type", gb.getRECORD_TYPE() + "");
		fieldNameValues.put("info:ni_pdp", gb.getNI_PDP() + "");
		fieldNameValues.put("info:msisdn", gb.getMSISDN() + "");
		fieldNameValues.put("info:imsi_number", gb.getIMSI_NUMBER() + "");
		fieldNameValues.put("info:sgsn", gb.getSGSN() + "");
		fieldNameValues.put("info:msnc", gb.getMSNC() + "");
		fieldNameValues.put("info:lac", gb.getLAC() + "");
		fieldNameValues.put("info:ra", gb.getRA() + "");
		fieldNameValues.put("info:cell_id", gb.getCELL_ID() + "");
		fieldNameValues.put("info:charging_id", gb.getCHARGING_ID() + "");
		fieldNameValues.put("info:ggsn", gb.getGGSN() + "");
		fieldNameValues.put("info:apnni", gb.getAPNNI() + "");
		fieldNameValues.put("info:apnoi", gb.getAPNOI() + "");
		fieldNameValues.put("info:pdp_type", gb.getPDP_TYPE() + "");
		fieldNameValues.put("info:spa", gb.getSPA() + "");
		fieldNameValues.put("info:sgsn_change", gb.getSGSN_CHANGE() + "");
		fieldNameValues.put("info:cause_close", gb.getCAUSE_CLOSE() + "");
		fieldNameValues.put("info:result", gb.getRESULT() + "");
		fieldNameValues.put("info:home_area_code", gb.getHOME_AREA_CODE() + "");
		fieldNameValues.put("info:visit_area_code", gb.getVISIT_AREA_CODE() + "");
		fieldNameValues.put("info:user_type", gb.getUSER_TYPE() + "");
		fieldNameValues.put("info:fee_type", gb.getFEE_TYPE() + "");
		fieldNameValues.put("info:roam_type", gb.getROAM_TYPE() + "");
		fieldNameValues.put("info:service_type", gb.getSERVICE_TYPE() + "");
		fieldNameValues.put("info:start_date", gb.getSTART_DATE() + "");
		fieldNameValues.put("info:start_time", gb.getSTART_TIME() + "");
		fieldNameValues.put("info:call_duration", gb.getCALL_DURATION() + "");
		fieldNameValues.put("info:tariff1", gb.getTARIFF1() + "");
		fieldNameValues.put("info:data_up1", gb.getDATA_UP1() + "");
		fieldNameValues.put("info:data_down1", gb.getDATA_DOWN1() + "");
		fieldNameValues.put("info:duration1", gb.getDURATION1() + "");
		fieldNameValues.put("info:tariff2", gb.getTARIFF2() + "");
		fieldNameValues.put("info:data_up2", gb.getDATA_UP2() + "");
		fieldNameValues.put("info:data_down2", gb.getDATA_DOWN2() + "");
		fieldNameValues.put("info:duration2", gb.getDURATION2() + "");
		fieldNameValues.put("info:tariff3", gb.getTARIFF3() + "");
		fieldNameValues.put("info:data_up3", gb.getDATA_UP3() + "");
		fieldNameValues.put("info:data_down3", gb.getDATA_DOWN3() + "");
		fieldNameValues.put("info:duration3", gb.getDURATION3() + "");
		fieldNameValues.put("info:tariff4", gb.getTARIFF4() + "");
		fieldNameValues.put("info:data_up4", gb.getDATA_UP4() + "");
		fieldNameValues.put("info:data_down4", gb.getDATA_DOWN4() + "");
		fieldNameValues.put("info:duration4", gb.getDURATION4() + "");
		fieldNameValues.put("info:tariff5", gb.getTARIFF5() + "");
		fieldNameValues.put("info:data_up5", gb.getDATA_UP5() + "");
		fieldNameValues.put("info:data_down5", gb.getDATA_DOWN5() + "");
		fieldNameValues.put("info:duration5", gb.getDURATION5() + "");
		fieldNameValues.put("info:tariff6", gb.getTARIFF6() + "");
		fieldNameValues.put("info:data_up6", gb.getDATA_UP6() + "");
		fieldNameValues.put("info:data_down6", gb.getDATA_DOWN6() + "");
		fieldNameValues.put("info:duration6", gb.getDURATION6() + "");
		fieldNameValues.put("info:cfee", gb.getCFEE() + "");
		fieldNameValues.put("info:fee1", gb.getFEE1() + "");
		fieldNameValues.put("info:fee2", gb.getFEE2() + "");
		fieldNameValues.put("info:fee3", gb.getFEE3() + "");
		fieldNameValues.put("info:total_fee", gb.getTOTAL_FEE() + "");
		fieldNameValues.put("info:deal_time", gb.getDEAL_TIME() + "");
		fieldNameValues.put("info:file_no", gb.getFILE_NO() + "");
		fieldNameValues.put("info:error_code", gb.getERROR_CODE() + "");
		fieldNameValues.put("info:cust_id", gb.getCUST_ID() + "");
		fieldNameValues.put("info:user_id", gb.getUSER_ID() + "");
		fieldNameValues.put("info:a_brand_id", gb.getA_BRAND_ID() + "");
		fieldNameValues.put("info:pay_mode", gb.getPAY_MODE() + "");
		fieldNameValues.put("info:a_serv_type", gb.getA_SERV_TYPE() + "");
		fieldNameValues.put("info:chan_no", gb.getCHAN_NO() + "");
		fieldNameValues.put("info:office_code", gb.getOFFICE_CODE() + "");
		fieldNameValues.put("info:free_code", gb.getFREE_CODE() + "");
		fieldNameValues.put("info:billing_cycle", gb.getBILLING_CYCLE() + "");
		fieldNameValues.put("info:bill_start_day", gb.getBILL_START_DAY() + "");
		fieldNameValues.put("info:revision", gb.getREVISION() + "");
		fieldNameValues.put("info:cdr_day", gb.getCDR_DAY() + "");
		fieldNameValues.put("info:file_pp_name", gb.getFILE_PP_NAME() + "");
		fieldNameValues.put("info:visit_county", gb.getVISIT_COUNTY() + "");
		fieldNameValues.put("info:deduct_remark", gb.getDEDUCT_REMARK() + "");
		fieldNameValues.put("info:deduct_remark2", gb.getDEDUCT_REMARK2() + "");
		fieldNameValues.put("info:imei", gb.getIMEI() + "");
		fieldNameValues.put("info:recordextension", gb.getRECORDEXTENSION() + "");
		fieldNameValues.put("info:billinfo", gb.getBILLINFO() + "");
		fieldNameValues.put("info:rcdseqnum", gb.getRCDSEQNUM() + "");
		fieldNameValues.put("info:apnni_groupid", gb.getAPNNI_GROUPID() + "");
		fieldNameValues.put("info:free_item1", gb.getFREE_ITEM1() + "");
		fieldNameValues.put("info:free1", gb.getFREE1() + "");
		fieldNameValues.put("info:product_id1", gb.getPRODUCT_ID1() + "");
		fieldNameValues.put("info:instance_id1", gb.getINSTANCE_ID1() + "");
		fieldNameValues.put("info:accu_user_id1", gb.getACCU_USER_ID1() + "");
		fieldNameValues.put("info:free_item2", gb.getFREE_ITEM2() + "");
		fieldNameValues.put("info:free2", gb.getFREE2() + "");
		fieldNameValues.put("info:product_id2", gb.getPRODUCT_ID2() + "");
		fieldNameValues.put("info:instance_id2", gb.getINSTANCE_ID2() + "");
		fieldNameValues.put("info:accu_user_id2", gb.getACCU_USER_ID2() + "");
		fieldNameValues.put("info:free_item3", gb.getFREE_ITEM3() + "");
		fieldNameValues.put("info:free3", gb.getFREE3() + "");
		fieldNameValues.put("info:product_id3", gb.getPRODUCT_ID3() + "");
		fieldNameValues.put("info:instance_id3", gb.getINSTANCE_ID3() + "");
		fieldNameValues.put("info:accu_user_id3", gb.getACCU_USER_ID3() + "");
		fieldNameValues.put("info:free_item4", gb.getFREE_ITEM4() + "");
		fieldNameValues.put("info:free4", gb.getFREE4() + "");
		fieldNameValues.put("info:product_id4", gb.getPRODUCT_ID4() + "");
		fieldNameValues.put("info:instance_id4", gb.getINSTANCE_ID4() + "");
		fieldNameValues.put("info:accu_user_id4", gb.getACCU_USER_ID4() + "");
		fieldNameValues.put("info:ratemethod", gb.getRATEMETHOD() + "");
		fieldNameValues.put("info:free_code1", gb.getFREE_CODE1() + "");
		fieldNameValues.put("info:free_fee1", gb.getFREE_FEE1() + "");
		fieldNameValues.put("info:rate_item_id1", gb.getRATE_ITEM_ID1() + "");
		fieldNameValues.put("info:free_code2", gb.getFREE_CODE2() + "");
		fieldNameValues.put("info:free_fee2", gb.getFREE_FEE2() + "");
		fieldNameValues.put("info:rate_item_id2", gb.getRATE_ITEM_ID2() + "");
		fieldNameValues.put("info:free_code3", gb.getFREE_CODE3() + "");
		fieldNameValues.put("info:free_fee3", gb.getFREE_FEE3() + "");
		fieldNameValues.put("info:rate_item_id3", gb.getRATE_ITEM_ID3() + "");
		fieldNameValues.put("info:free_code4", gb.getFREE_CODE4() + "");
		fieldNameValues.put("info:free_fee4", gb.getFREE_FEE4() + "");
		fieldNameValues.put("info:rate_item_id4", gb.getRATE_ITEM_ID4() + "");
		fieldNameValues.put("info:base_tpremark", gb.getBASE_TPREMARK() + "");
		fieldNameValues.put("info:tpremark", gb.getTPREMARK() + "");
		fieldNameValues.put("info:rate_file_name", gb.getRATE_FILE_NAME() + "");
		fieldNameValues.put("info:rate_time", gb.getRATE_TIME() + "");
		fieldNameValues.put("info:acc_file_name", gb.getACC_FILE_NAME() + "");
		fieldNameValues.put("info:acc_time", gb.getACC_TIME() + "");
		fieldNameValues.put("info:dis_file_name", gb.getDIS_FILE_NAME() + "");
		fieldNameValues.put("info:dis_time", gb.getDIS_TIME() + "");
		fieldNameValues.put("info:busi_domain", gb.getBUSI_DOMAIN() + "");
		fieldNameValues.put("info:bill_end_day", gb.getBILL_END_DAY() + "");
		return fieldNameValues;
	}
}
