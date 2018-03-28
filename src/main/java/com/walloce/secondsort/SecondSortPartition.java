package com.walloce.secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 自定义分区（根据年份分区）
 * @author Walloce
 * 2018
 */
public class SecondSortPartition extends Partitioner<SecondKey, NullWritable> {

	@Override
	public int getPartition(SecondKey key, NullWritable value, int numPartitions) {
		
		
		// TODO Auto-generated method stub
		System.out.println("进入自定义分区阶段....");
		if(key.getYear()==1999){
			return 0;
		}
		if(key.getYear()==2000){
			return 1;
		}
		if(key.getYear()==2001){
			return 2;
		}
		return 0;
	}
	
}
