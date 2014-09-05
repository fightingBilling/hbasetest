package com.wankun.hbasetest.util;

public class RowCounter {
	protected long rownum = 0;

	public long getRownum() {
		return rownum;
	}
	
	public void countUp(){
		this.rownum = this.rownum+1;
	}

}
