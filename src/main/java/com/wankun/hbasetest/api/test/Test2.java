package com.wankun.hbasetest.api.test;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.wankun.hbasetest.util.GprsBean;
import com.wankun.hbasetest.util.LogMessage;
import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.RowCounter;
/**
 * 
 * @author wankun
 *
 */
public class Test2 {
	static DecimalFormat rowKeyFormatter = new DecimalFormat("00000000");
	// 生产数据的现场
	private MakeData md;
	private Thread mdth;
	// 记录日志的线程
	private RowCounter rowCounter;
	private LogMessage logmsg;
	private Thread logmsgth;

	public Test2() {
		md = new MakeData();
		mdth = new Thread(md);
		rowCounter = new RowCounter();
		logmsg = new LogMessage(rowCounter);
		logmsgth = new Thread(logmsg);
	}

	public static void main(String[] args) {
		Test2 test = new Test2();
		test.runTest();
	}

	public void runTest() {
		mdth.start();
		logmsgth.start();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = new Date();
		System.out.println("开始测试时间：" + sdf.format(d1));
		for (long i = 0; i < 1000000; i++) {
			// 如果运行出错，直接提过，这样在结果集中rowcount会小于1000000
			try {
				String rowKey = rowKeyFormatter.format(i);
				Put put = genPut(rowKey);
				rowCounter.countUp();
			} catch (InterruptedException e) {
				continue;
			}
		}
		
		logmsg.setRunflag(false);
		md.setRunflag(false);
		
		Date d2 = new Date();
		int runtime = (int) (d2.getTime() - d1.getTime()) / 1000;
		System.out.println("结束测试时间：" + sdf.format(d2) + " 测试运行时间：" + runtime + " 成功插入数据量：" + rowCounter.getRownum());
	}
	
	public Put genPut(String rowKey) throws InterruptedException {
		Put put = new Put(Bytes.toBytes(rowKey));
		GprsBean gb = md.getQueue().take();
		put.add(Bytes.toBytes("info"), Bytes.toBytes("source_type"), Bytes.toBytes(gb.getSOURCE_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("record_type"), Bytes.toBytes(gb.getRECORD_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("ni_pdp"), Bytes.toBytes(gb.getNI_PDP()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("msisdn"), Bytes.toBytes(gb.getMSISDN()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("imsi_number"), Bytes.toBytes(gb.getIMSI_NUMBER()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("sgsn"), Bytes.toBytes(gb.getSGSN()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("msnc"), Bytes.toBytes(gb.getMSNC()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("lac"), Bytes.toBytes(gb.getLAC()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("ra"), Bytes.toBytes(gb.getRA()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("cell_id"), Bytes.toBytes(gb.getCELL_ID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("charging_id"), Bytes.toBytes(gb.getCHARGING_ID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("ggsn"), Bytes.toBytes(gb.getGGSN()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("apnni"), Bytes.toBytes(gb.getAPNNI()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("apnoi"), Bytes.toBytes(gb.getAPNOI()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("pdp_type"), Bytes.toBytes(gb.getPDP_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("spa"), Bytes.toBytes(gb.getSPA()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("sgsn_change"), Bytes.toBytes(gb.getSGSN_CHANGE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("cause_close"), Bytes.toBytes(gb.getCAUSE_CLOSE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("result"), Bytes.toBytes(gb.getRESULT()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("home_area_code"), Bytes.toBytes(gb.getHOME_AREA_CODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("visit_area_code"), Bytes.toBytes(gb.getVISIT_AREA_CODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("user_type"), Bytes.toBytes(gb.getUSER_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("fee_type"), Bytes.toBytes(gb.getFEE_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("roam_type"), Bytes.toBytes(gb.getROAM_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("service_type"), Bytes.toBytes(gb.getSERVICE_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("start_date"), Bytes.toBytes(gb.getSTART_DATE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("start_time"), Bytes.toBytes(gb.getSTART_TIME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("call_duration"), Bytes.toBytes(gb.getCALL_DURATION()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff1"), Bytes.toBytes(gb.getTARIFF1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up1"), Bytes.toBytes(gb.getDATA_UP1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down1"), Bytes.toBytes(gb.getDATA_DOWN1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration1"), Bytes.toBytes(gb.getDURATION1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff2"), Bytes.toBytes(gb.getTARIFF2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up2"), Bytes.toBytes(gb.getDATA_UP2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down2"), Bytes.toBytes(gb.getDATA_DOWN2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration2"), Bytes.toBytes(gb.getDURATION2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff3"), Bytes.toBytes(gb.getTARIFF3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up3"), Bytes.toBytes(gb.getDATA_UP3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down3"), Bytes.toBytes(gb.getDATA_DOWN3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration3"), Bytes.toBytes(gb.getDURATION3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff4"), Bytes.toBytes(gb.getTARIFF4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up4"), Bytes.toBytes(gb.getDATA_UP4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down4"), Bytes.toBytes(gb.getDATA_DOWN4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration4"), Bytes.toBytes(gb.getDURATION4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff5"), Bytes.toBytes(gb.getTARIFF5()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up5"), Bytes.toBytes(gb.getDATA_UP5()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down5"), Bytes.toBytes(gb.getDATA_DOWN5()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration5"), Bytes.toBytes(gb.getDURATION5()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tariff6"), Bytes.toBytes(gb.getTARIFF6()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_up6"), Bytes.toBytes(gb.getDATA_UP6()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("data_down6"), Bytes.toBytes(gb.getDATA_DOWN6()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("duration6"), Bytes.toBytes(gb.getDURATION6()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("cfee"), Bytes.toBytes(gb.getCFEE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("fee1"), Bytes.toBytes(gb.getFEE1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("fee2"), Bytes.toBytes(gb.getFEE2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("fee3"), Bytes.toBytes(gb.getFEE3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("total_fee"), Bytes.toBytes(gb.getTOTAL_FEE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("deal_time"), Bytes.toBytes(gb.getDEAL_TIME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("file_no"), Bytes.toBytes(gb.getFILE_NO()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("error_code"), Bytes.toBytes(gb.getERROR_CODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("cust_id"), Bytes.toBytes(gb.getCUST_ID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("user_id"), Bytes.toBytes(gb.getUSER_ID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("a_brand_id"), Bytes.toBytes(gb.getA_BRAND_ID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("pay_mode"), Bytes.toBytes(gb.getPAY_MODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("a_serv_type"), Bytes.toBytes(gb.getA_SERV_TYPE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("chan_no"), Bytes.toBytes(gb.getCHAN_NO()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("office_code"), Bytes.toBytes(gb.getOFFICE_CODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_code"), Bytes.toBytes(gb.getFREE_CODE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("billing_cycle"), Bytes.toBytes(gb.getBILLING_CYCLE()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("bill_start_day"), Bytes.toBytes(gb.getBILL_START_DAY()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("revision"), Bytes.toBytes(gb.getREVISION()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("cdr_day"), Bytes.toBytes(gb.getCDR_DAY()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("file_pp_name"), Bytes.toBytes(gb.getFILE_PP_NAME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("visit_county"), Bytes.toBytes(gb.getVISIT_COUNTY()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("deduct_remark"), Bytes.toBytes(gb.getDEDUCT_REMARK()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("deduct_remark2"), Bytes.toBytes(gb.getDEDUCT_REMARK2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("imei"), Bytes.toBytes(gb.getIMEI()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("recordextension"), Bytes.toBytes(gb.getRECORDEXTENSION()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("billinfo"), Bytes.toBytes(gb.getBILLINFO()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rcdseqnum"), Bytes.toBytes(gb.getRCDSEQNUM()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("apnni_groupid"), Bytes.toBytes(gb.getAPNNI_GROUPID()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_item1"), Bytes.toBytes(gb.getFREE_ITEM1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free1"), Bytes.toBytes(gb.getFREE1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("product_id1"), Bytes.toBytes(gb.getPRODUCT_ID1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("instance_id1"), Bytes.toBytes(gb.getINSTANCE_ID1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("accu_user_id1"), Bytes.toBytes(gb.getACCU_USER_ID1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_item2"), Bytes.toBytes(gb.getFREE_ITEM2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free2"), Bytes.toBytes(gb.getFREE2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("product_id2"), Bytes.toBytes(gb.getPRODUCT_ID2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("instance_id2"), Bytes.toBytes(gb.getINSTANCE_ID2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("accu_user_id2"), Bytes.toBytes(gb.getACCU_USER_ID2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_item3"), Bytes.toBytes(gb.getFREE_ITEM3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free3"), Bytes.toBytes(gb.getFREE3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("product_id3"), Bytes.toBytes(gb.getPRODUCT_ID3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("instance_id3"), Bytes.toBytes(gb.getINSTANCE_ID3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("accu_user_id3"), Bytes.toBytes(gb.getACCU_USER_ID3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_item4"), Bytes.toBytes(gb.getFREE_ITEM4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free4"), Bytes.toBytes(gb.getFREE4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("product_id4"), Bytes.toBytes(gb.getPRODUCT_ID4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("instance_id4"), Bytes.toBytes(gb.getINSTANCE_ID4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("accu_user_id4"), Bytes.toBytes(gb.getACCU_USER_ID4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("ratemethod"), Bytes.toBytes(gb.getRATEMETHOD()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_code1"), Bytes.toBytes(gb.getFREE_CODE1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_fee1"), Bytes.toBytes(gb.getFREE_FEE1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_item_id1"), Bytes.toBytes(gb.getRATE_ITEM_ID1()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_code2"), Bytes.toBytes(gb.getFREE_CODE2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_fee2"), Bytes.toBytes(gb.getFREE_FEE2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_item_id2"), Bytes.toBytes(gb.getRATE_ITEM_ID2()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_code3"), Bytes.toBytes(gb.getFREE_CODE3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_fee3"), Bytes.toBytes(gb.getFREE_FEE3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_item_id3"), Bytes.toBytes(gb.getRATE_ITEM_ID3()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_code4"), Bytes.toBytes(gb.getFREE_CODE4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("free_fee4"), Bytes.toBytes(gb.getFREE_FEE4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_item_id4"), Bytes.toBytes(gb.getRATE_ITEM_ID4()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("base_tpremark"), Bytes.toBytes(gb.getBASE_TPREMARK()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tpremark"), Bytes.toBytes(gb.getTPREMARK()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_file_name"), Bytes.toBytes(gb.getRATE_FILE_NAME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("rate_time"), Bytes.toBytes(gb.getRATE_TIME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("acc_file_name"), Bytes.toBytes(gb.getACC_FILE_NAME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("acc_time"), Bytes.toBytes(gb.getACC_TIME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("dis_file_name"), Bytes.toBytes(gb.getDIS_FILE_NAME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("dis_time"), Bytes.toBytes(gb.getDIS_TIME()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("busi_domain"), Bytes.toBytes(gb.getBUSI_DOMAIN()));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("bill_end_day"), Bytes.toBytes(gb.getBILL_END_DAY()));
		return put;
	}

}
