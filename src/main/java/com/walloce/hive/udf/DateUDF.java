package com.walloce.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Title: MyHiveUDF.java  
 * @Package com.walloce.hive.udf  
 * @Description: 定义日期转换的UDF 
 * @author Walloce  
 * @date 2018年4月10日
 */
public class DateUDF extends UDF {
	
	/*
	 * 定义转换前的日期格式（即数据需要转换的日期）
	 * 输入前："31/Aug/2015:00:04:37 +0800"
	 * 转换后的期望值：2015-08-31 00:04:37
	 */
	public SimpleDateFormat inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	
	public SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public String evaluate(String time) {
		
		//接收转换后的时间字符串
		String outputTime = null;
		
		//判断非空
		if (time ==null || StringUtils.isBlank(time.toString())) {
			
			return null;
		}
		
		String paresTime = time.toString().replaceAll("\"", "");
		
		try {
			Date praseDate = inputDate.parse(paresTime);
			outputTime = outputDate.format(praseDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return outputTime;
	}
}
