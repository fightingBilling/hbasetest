package com.wankun.hbasetest.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 负责记录线程状态
 * @author wankun
 *
 */
public class LogMessage implements Runnable {
	Log log = LogFactory.getLog(LogMessage.class);
	private Boolean runflag=true;
	public static final int SECOND = 1000;
	private RowCounter rowCounter ;

	public LogMessage(RowCounter rowCounter){
		this.rowCounter = rowCounter;
	}
	
	public Boolean getRunflag() {
		return runflag;
	}

	public void setRunflag(Boolean runflag) {
		this.runflag = runflag;
	}

	@Override
	public void run() {
		long lastcount = 0;
		while (runflag) {
			log.info("LogMessage[allrow] : " + rowCounter.getRownum() + "\t\t  [output]" + (rowCounter.getRownum() - lastcount));
			lastcount = rowCounter.getRownum();
			try {
				Thread.currentThread().sleep(SECOND);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
