package com.uniplore.table;
/**
 * 列信息类，用于筛选合适的列值
 * @author tian
 *
 */
public class ColumnDescInfo {
	
	private String colName;//列名
	private String colValue;//列值
	private long num;//出现概率
	private long minV;//最小偏移量
	private long maxV;//最大偏移量
	

	public ColumnDescInfo() {
	}

	public ColumnDescInfo(String colName, String colValue, long num) {
		super();
		this.colName = colName;
		this.colValue = colValue;
		this.num = num;
	}




	public String getColName() {
		return colName;
	}


	public void setColName(String colName) {
		this.colName = colName;
	}


	public String getColValue() {
		return colValue;
	}


	public void setColValue(String colValue) {
		this.colValue = colValue;
	}


	public long getNum() {
		return num;
	}


	public void setNum(long num) {
		this.num = num;
	}


	public long getMinV() {
		return minV;
	}


	public void setMinV(long minV) {
		this.minV = minV;
	}


	public long getMaxV() {
		return maxV;
	}


	public void setMaxV(long maxV) {
		this.maxV = maxV;
	}
	
	/**
	 * 检查值value是否符合值范围，以确认该列值为当前值
	 * @param value
	 * @return
	 */
	public boolean getFlag(long value){
		if((value>=this.minV)&&(value<=this.maxV)){
			return true;
		}else{
			return false;
		}
	}

	@Override
	public String toString() {
		return "ColumnDescInfo [colName=" + colName + ", colValue=" + colValue + ", num=" + num + ", minV=" + minV
				+ ", maxV=" + maxV + "]";
	}

	
	
}
