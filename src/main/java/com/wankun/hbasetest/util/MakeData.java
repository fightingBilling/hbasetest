package com.wankun.hbasetest.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 负责产生测试数据
 * @author wankun
 *
 */
public class MakeData implements Runnable {

	Log log = LogFactory.getLog(MakeData.class);
	private Boolean runflag=true;
	private BlockingQueue<GprsBean> queue = new LinkedBlockingQueue<GprsBean>(500);
	public MakeData() {
	}

	public BlockingQueue<GprsBean> getQueue() {
		return queue;
	}

	@Override
	public void run() {
		while (runflag) {
			synchronized (queue) {
				if(queue.size()< 500){
					GprsBean gb = genBean();
					try {
						queue.put(gb);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	static DecimalFormat formatter = new DecimalFormat("00");

	public static String randomlyBirthday() {
		Random r = new Random();
		int year = 1900 + r.nextInt(100);
		int month = 1 + r.nextInt(12);
		int date = 1 + r.nextInt(30);
		return String.valueOf(year + "-" + formatter.format(month) + "-" + formatter.format(date));
	}

	public static String randomlyGender() {
		Random r = new Random();
		int flag = r.nextInt(2);
		return flag == 0 ? "M" : "F";
	}

	public static String randomlyUserType() {
		Random r = new Random();
		int flag = 1 + r.nextInt(10);
		return String.valueOf(flag);
	}
	
	private GprsBean genBean(){
		GprsBean gb = new GprsBean();
		gb.setA_BRAND_ID((int) (Math.random() * 100));
		gb.setA_SERV_TYPE("13");
		gb.setACC_FILE_NAME("null");
		gb.setACC_TIME("null");
		gb.setACCU_USER_ID1(String.valueOf((long)(Math.random() *100000000) *  10000000));
		gb.setACCU_USER_ID2(String.valueOf((long)(Math.random() * 100000000) * 10000000));
		gb.setACCU_USER_ID3(String.valueOf((long)(Math.random() * 100000000 * 10000000)));
		gb.setACCU_USER_ID4(String.valueOf((long)(Math.random() * 100000000 * 10000000)));
		gb.setAPNNI("cmnet");
		gb.setAPNNI_GROUPID(String.valueOf((int)(Math.random() * 100)));
		gb.setAPNOI("null");
		gb.setBASE_TPREMARK("0|0000000000|2381:3144|;");
		gb.setBILL_END_DAY(20130531);// 账期结束时间 是否随机？
		gb.setBILL_START_DAY(20130501);
		gb.setBILLINFO("null");
		gb.setBILLING_CYCLE(String.valueOf(201305));
		gb.setBUSI_DOMAIN("null");
		gb.setCALL_DURATION((int) (Math.random() * 100000));
		gb.setCAUSE_CLOSE(String.valueOf((int)(Math.random() * 10)));// ////////////////////
		gb.setCDR_DAY(Integer.parseInt((new SimpleDateFormat("yyyyMMdd")).format(randomDate("2013-10-01", "2013-10-31"))));// 是否要随机
		gb.setCELL_ID("null");
		gb.setCFEE((long) ((int)(Math.random() * 1000000)));
		gb.setCHAN_NO("N001");// 是否需要随机
		gb.setCHARGING_ID(String.valueOf((long)(Math.random() * 100000000 * 10000000)));
		gb.setCUST_ID("null");
		gb.setDATA_DOWN1((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_DOWN2((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_DOWN3((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_DOWN4((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_DOWN5((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_DOWN6((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP1((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP2((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP3((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP4((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP5((long) ((int)(Math.random() * 1000000)));
		gb.setDATA_UP6((long) ((int)(Math.random() * 1000000)));
		gb.setDEAL_TIME(String.valueOf((int)(Math.random() * 100000000)));// 处理时间秒数
		gb.setDEDUCT_REMARK("null");
		gb.setDEDUCT_REMARK2("##");
		gb.setDIS_FILE_NAME("DGGP");
		gb.setDIS_TIME("null");
		gb.setDURATION1((int) (Math.random() * 1000000));
		gb.setDURATION2((int) (Math.random() * 1000000));
		gb.setDURATION3((int) (Math.random() * 1000000));
		gb.setDURATION4((int) (Math.random() * 1000000));
		gb.setDURATION5((int) (Math.random() * 1000000));
		gb.setDURATION6((int) (Math.random() * 1000000));
		gb.setERROR_CODE(String.valueOf((int)(Math.random() * 10000000)));
		gb.setFEE1((long) 0);
		gb.setFEE2((long) 0);
		gb.setFEE3((long) 0);
		gb.setFEE_TYPE(String.valueOf((int)Math.random() * 10));
		gb.setFILE_NO("0GGEA7105031355.3312");
		gb.setFILE_PP_NAME("null");
		gb.setFREE1((long) ((int)(Math.random() * 1000000)));
		gb.setFREE2((long) ((int)(Math.random() * 1000000)));
		gb.setFREE3((long) ((int)(Math.random() * 1000000)));
		gb.setFREE4((long) ((int)(Math.random() * 1000000)));
		gb.setFREE_CODE(String.valueOf((int)(Math.random() * 100000000)));
		gb.setFREE_CODE1(String.valueOf((int)(Math.random() * 100000000)));
		gb.setFREE_CODE2(String.valueOf((int)(Math.random() * 100000000)));
		gb.setFREE_CODE3(String.valueOf((int)(Math.random() * 100000000)));
		gb.setFREE_CODE4(String.valueOf((int)(Math.random() * 100000000)));
		gb.setFREE_FEE1(0);
		gb.setFREE_FEE2(0);
		gb.setFREE_FEE3(0);
		gb.setFREE_FEE4(0);
		gb.setFREE_ITEM1((int) (Math.random() * 10000));
		gb.setFREE_ITEM2((int) (Math.random() * 10000));
		gb.setFREE_ITEM3((int) (Math.random() * 10000));
		gb.setFREE_ITEM4((int) (Math.random() * 10000));
		gb.setGGSN("DDB18B82");
		gb.setHOME_AREA_CODE(String.valueOf((int)(Math.random() * 1000)));// 归属地
		gb.setIMEI(String.valueOf((long)(Math.random() * 100000000 * 100000000)));// IMEI
																				// 16位
		gb.setIMSI_NUMBER(String.valueOf((long)(Math.random() * 100000000 * 10000000)));// IMSI
																						// 15位
		gb.setINSTANCE_ID1(String.valueOf((long)(Math.random() * 100000000 * 100000)));
		gb.setINSTANCE_ID2(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setINSTANCE_ID3(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setINSTANCE_ID4(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setLAC("null");
		gb.setMSISDN(String.valueOf((long)(Math.random() * 100000000 * 1000)));// 手机号
		gb.setMSNC('2');
		gb.setNI_PDP('0');
		gb.setOFFICE_CODE(String.valueOf((int)(Math.random() * 10000)));// 归属地市
		gb.setPAY_MODE((short)1);//
		gb.setPDP_TYPE('1');
		gb.setPRODUCT_ID1(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setPRODUCT_ID2(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setPRODUCT_ID3(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setPRODUCT_ID4(String.valueOf((long)(Math.random() * 100000000 * 1000000)));
		gb.setRA("null");
		gb.setRATE_FILE_NAME("null");
		gb.setRATE_ITEM_ID1(String.valueOf(0));
		gb.setRATE_ITEM_ID2(String.valueOf(0));
		gb.setRATE_ITEM_ID3(String.valueOf(0));
		gb.setRATE_ITEM_ID4(String.valueOf(0));
		gb.setRATEMETHOD('1');
		gb.setRCDSEQNUM((int) (Math.random() * 100000000));
		gb.setRECORD_TYPE('1');
		gb.setRECORDEXTENSION("2000000002|0|3762|784|0");
		gb.setRESULT('1');
		gb.setREVISION((short)(int) (Math.random() * 10));
		gb.setROAM_TYPE('0');
		gb.setSERVICE_TYPE(String.valueOf((int)(Math.random() * 100)));
		gb.setSGSN("DDB1D690");
		gb.setSGSN_CHANGE('X');
		gb.setSOURCE_TYPE("M");
		gb.setSPA("0AF3CE0F");
		gb.setSTART_DATE(String.valueOf(gb.getCDR_DAY()));
		gb.setSTART_TIME((new SimpleDateFormat("hhmmss")).format(randomTime("00:00:00", "24:59:59")));
		gb.setTARIFF1('A');
		gb.setTARIFF2('B');
		gb.setTARIFF3('C');
		gb.setTARIFF4('D');
		gb.setTARIFF5('E');
		gb.setTARIFF6('F');
		gb.setTOTAL_FEE((long) 0);
		gb.setTPREMARK("31441|1|0|1#");
		gb.setUSER_ID((long) ((long)(Math.random() *100000000 *100000000)));
		gb.setUSER_TYPE(String.valueOf(0));
		gb.setVISIT_AREA_CODE(String.valueOf((int)(Math.random() * 1000)));// 到访地区号
		gb.setVISIT_COUNTY("null");
		gb.setRATE_TIME((new SimpleDateFormat("hhmmss")).format(randomTime("00:00:00", "24:59:59")));
		return gb;
	}
	
	
	private static Date randomDate(String beginDate, String endDate) {
	    try {
	        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	        Date start = format.parse(beginDate);// 开始日期
	        Date end = format.parse(endDate);// 结束日期
	        if (start.getTime() >= end.getTime()) {
	            return null;
	        }
	        long date = random(start.getTime(), end.getTime());
	        return new Date(date);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return null;
	}

	private static Date randomTime(String beginDate, String endDate) {
	    try {
	        SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss");
	        Date start = format.parse(beginDate);// 开始日期
	        Date end = format.parse(endDate);// 结束日期
	        if (start.getTime() >= end.getTime()) {
	            return null;
	        }
	        long date = random(start.getTime(), end.getTime());
	        return new Date(date);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return null;
	}

	private static long random(long begin, long end) {
	    long rtnn = begin + (long) (Math.random() * (end - begin));
	    if (rtnn == begin || rtnn == end) {
	        return random(begin, end);
	    }
	    return rtnn;
	  }

	public Boolean getRunflag() {
		return runflag;
	}

	public void setRunflag(Boolean runflag) {
		this.runflag = runflag;
	}

	public static Put genPut(MakeData md,String rowKey) throws InterruptedException {
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
	
	public static Get genGet(String rowKey) throws InterruptedException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("source_type")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("record_type")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ni_pdp")            );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("msisdn")            );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("imsi_number")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sgsn")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("msnc")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("lac")               );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ra")                );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cell_id")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("charging_id")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ggsn")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("apnni")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("apnoi")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("pdp_type")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("spa")               );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sgsn_change")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cause_close")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("result")            );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("home_area_code")    );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("visit_area_code")   );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("user_type")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("fee_type")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("roam_type")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("service_type")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("start_date")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("start_time")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("call_duration")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff1")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up1")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down1")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration1")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff2")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up2")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down2")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration2")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff3")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up3")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down3")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration3")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff4")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up4")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down4")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration4")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff5")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up5")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down5")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration5")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tariff6")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_up6")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data_down6")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("duration6")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cfee")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("fee1")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("fee2")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("fee3")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("total_fee")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deal_time")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("file_no")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("error_code")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cust_id")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("user_id")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("a_brand_id")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("pay_mode")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("a_serv_type")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("chan_no")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("office_code")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_code")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("billing_cycle")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("bill_start_day")    );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("revision")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cdr_day")           );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("file_pp_name")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("visit_county")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deduct_remark")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deduct_remark2")    );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("imei")              );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("recordextension")   );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("billinfo")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rcdseqnum")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("apnni_groupid")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_item1")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free1")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("product_id1")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("instance_id1")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("accu_user_id1")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_item2")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free2")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("product_id2")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("instance_id2")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("accu_user_id2")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_item3")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free3")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("product_id3")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("instance_id3")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("accu_user_id3")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_item4")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free4")             );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("product_id4")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("instance_id4")      );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("accu_user_id4")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ratemethod")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_code1")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_fee1")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_item_id1")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_code2")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_fee2")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_item_id2")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_code3")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_fee3")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_item_id3")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_code4")        );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("free_fee4")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_item_id4")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("base_tpremark")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tpremark")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_file_name")    );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rate_time")         );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("acc_file_name")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("acc_time")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("dis_file_name")     );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("dis_time")          );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("busi_domain")       );
		get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("bill_end_day")      );
		return get;
	}
	
}
