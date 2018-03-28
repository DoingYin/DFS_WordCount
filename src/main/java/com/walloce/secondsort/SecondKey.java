package com.walloce.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondKey implements WritableComparable<SecondKey> {

	/**
	 * 年份
	 */
	private int year;
	
	/**
	 * 温度
	 */
	private double tmp;
	
	/**
	 * 日期字符串
	 */
	private String date;

	public SecondKey() {
		
	}
	
	public SecondKey(int year, double tmp, String date) {
		this.year = year;
		this.tmp = tmp;
		this.date = date;
	}
 	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public double getTmp() {
		return tmp;
	}

	public void setTmp(double tmp) {
		this.tmp = tmp;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public void setAll(int year, double tmp, String date) {
		this.year = year;
		this.tmp = tmp;
		this.date = date;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeDouble(tmp);
		out.writeUTF(date);
	}

	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.tmp = in.readDouble();
		this.date = in.readUTF();
	}

	public int compareTo(SecondKey arg0) {
		System.out.println("进入排序阶段。。。");
		//比较年份相同的时候，再对温度进行排序
		if (this.year == arg0.year) {
			return this.tmp - arg0.tmp > 0 ? 1 : -1;
		}
		return this.year - arg0.year;
	}

	@Override
	public String toString() {
		
		return "年份："+ this.year + "\t温度："+ this.tmp +"\t时间："+ this.date;
	}
}
