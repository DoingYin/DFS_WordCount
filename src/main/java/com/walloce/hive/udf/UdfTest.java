package com.walloce.hive.udf;

import org.apache.hadoop.io.Text;

/**
 * 
 * @Title: UdfTest.java  
 * @Package com.walloce.hive.udf  
 * @Description: 测试类
 * @author Walloce  
 * @date 2018年4月11日
 */
public class UdfTest {

	public static void main(String[] args) {
		
		String date = "31/Aug/2015:00:04:37 +0800";
		String dateFormated = new DateUDF().evaluate(date);
		System.out.println(dateFormated.toString());
	}

}
