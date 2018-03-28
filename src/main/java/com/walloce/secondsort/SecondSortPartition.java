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
//	HashPartitioner<K, V>
	@Override
	public int getPartition(SecondKey key, NullWritable value, int numPartitions) {
		
		
		// TODO Auto-generated method stub
		System.out.println("进入自定义分区阶段....");
		System.out.println("=============");
		System.out.println("进入到自定义的分区方法中");
		System.out.println("=============");
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
		/*if (yearsMap != null) {
			Integer oldyear = yearsMap.get(key.getYear());
			if (oldyear == null) {
				yearsMap.put(key.getYear(), counter);
			}
			counter ++;
			return yearsMap.get(key.getYear());
		} else {
			yearsMap.put(key.getYear(), counter);
			counter ++;
			return 0;
		}*/
	}
	
}
