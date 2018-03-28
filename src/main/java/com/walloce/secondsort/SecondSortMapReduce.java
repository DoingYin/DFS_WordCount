package com.walloce.secondsort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.walloce.secondsort.SecondSortMapReduce.myMap.myReduce;


public class SecondSortMapReduce extends Configured implements Tool {
	
	/**
	 * KEYIN LongWritable 传入的key类型(偏移量)
	 * VALUEIN Text 传入的value类型(文本)
	 * KEYOUT 传出的key类型
	 * VALUEOUT 传出的value类型
	 * NullWritable 设置输出的value为空（不显示输出的值）
	 * @author Walloce
	 * 2018
	 */
	static class myMap extends Mapper<LongWritable, Text, SecondKey, NullWritable> {
		//输出结果的key
		SecondKey secondKey = new SecondKey();
		//输出结果的value
		NullWritable mr_value = NullWritable.get();
		
		int line_count = 1;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			System.out.println("map阶段开始...");
			//将获取的文本类型转为字符串类型
			String line = value.toString();
			
			//将的到的一行字符串拆解为多个单词的字符串数组
			String[] words = line.split("\t");
			
			int year = Integer.parseInt(words[0]);
			double tmp = Double.parseDouble(words[1]);
			String date = words[2];
			secondKey.setAll(year, tmp, date);
			context.write(secondKey, mr_value);
		}
		
		/**
		 * Text, IntWritable, Text, IntWritable
		 * reduce时输入的key-value和输出的key-value.
		 * eg:(hello,2)
		 * @author Walloce
		 * 2018
		 */
		static class myReduce extends Reducer<SecondKey, NullWritable, SecondKey, NullWritable> {
			
			private NullWritable result = NullWritable.get();

			@Override
			protected void reduce(SecondKey key, Iterable<NullWritable> values, Context context)
					throws IOException, InterruptedException {
				
				System.out.println("Reduce阶段开始....");
				
				context.write(key, result);
			}
			
		}
		
		/**
		 * Text：输入的key
		 * IntWritable：输入的value 
		 * Text：输出的key 
		 * IntWritable:输出的value
		 * @author Walloce
		 * 2018
		 */
		static class myCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
			
			private IntWritable result = new IntWritable();

			@Override
			protected void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				System.out.println("Combiner阶段开始...");
				System.out.println(key +"--->"+ values.toString());
				int sum = 0;
				int counter = 0;
				for(IntWritable value : values) {
					sum += value.get();
					counter++;
				}
				result.set(sum);
				System.out.println(key +"===>"+ result.toString() +"==="+ counter);
				context.write(key, result);
			}
		}

	}
	
	public int run(String[] args) throws Exception {
		
		//hadoop的八股文
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//对job进行具体的配置
		
		//当你本地运行，这个设置可以不写，不会报错
		//当提价到集群上面运行的时候，这个设置不写，会报类找不到的异常 
		job.setJarByClass(SecondSortMapReduce.class);
		
		//写一个输入路径
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);
		//写一个输出路径
		Path output = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, output);
		
		//执行前先判断输出路径是否存在，存在就删除
		FileSystem fs = output.getFileSystem(conf);
		if(fs.exists(output)){
			fs.delete(output,true);
		}
		
		//设置运行的map类的相关参数
		job.setMapperClass(myMap.class);
		job.setMapOutputKeyClass(SecondKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		//==============shuffle=======================
		//job.setCombinerClass(myCombiner.class);
		job.setPartitionerClass(SecondSortPartition.class);
		job.setGroupingComparatorClass(SecondSortGroup.class);
		
		//==============shuffle=======================
		//设置运行的Reduce的相关参数
		job.setReducerClass(myReduce.class);
		job.setOutputKeyClass(SecondKey.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(3);
		boolean isSuccess =  job.waitForCompletion(true);
		
		
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://bigdata-study-104:8020/testdata/secondsort.txt",
				"hdfs://bigdata-study-104:8020/testresult/secondsort/"
		};

		try {
			int result = ToolRunner.run(conf, new SecondSortMapReduce(), args);
			System.out.println(result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
