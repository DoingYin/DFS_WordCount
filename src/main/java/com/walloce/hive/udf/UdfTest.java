package com.walloce.hive.udf;

import org.apache.hadoop.io.Text;

public class UdfTest {

	public static void main(String[] args) {
		String date = "31/Aug/2015:00:04:37 +0800";
		String dateFormated = new DateUDF().evaluate(date);
		System.out.println(dateFormated.toString());
	}

}
