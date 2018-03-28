package com.walloce.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortGroup extends WritableComparator {
	
	public  SecondSortGroup() {
		super(SecondKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		System.out.println("_______________________");
		System.out.println("进入分组阶段...");
		return super.compare(a, b);
	}
	
}
