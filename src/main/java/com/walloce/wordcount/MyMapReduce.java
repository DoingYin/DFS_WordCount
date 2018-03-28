package com.walloce.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.walloce.wordcount.MyMapReduce.myMap.myCombiner;
import com.walloce.wordcount.MyMapReduce.myMap.myReduce;

public class MyMapReduce extends Configured implements Tool {
	
	/**
	 * KEYIN LongWritable 传入的key类型(偏移量)
	 * VALUEIN Text 传入的value类型(文本)
	 * KEYOUT 传出的key类型
	 * VALUEOUT 传出的value类型
	 * @author Walloce
	 * 2018
	 */
	static class myMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		//输出结果的key
		Text text = new Text();
		//输出结果的value
		IntWritable mr_value = new IntWritable(1);
		
		int line_count = 1;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			System.out.println("map阶段开始...");
			//将获取的文本类型转为字符串类型
			String line = value.toString();
			
			System.out.println("第 "+ line_count +" 行的字符串的偏移量为：" + key.get());
			
			//将的到的一行字符串拆解为多个单词的字符串数组
			String[] words = line.split(" ");
			
			//遍历得到的所有word
			for (String word : words) {
				text.set(word);
				context.write(text, mr_value);
			}
			System.out.println(text +"->"+ mr_value);
			line_count++;
		}
		
		/**
		 * Text, IntWritable, Text, IntWritable
		 * reduce时输入的key-value和输出的key-value.
		 * eg:(hello,2)
		 * @author Walloce
		 * 2018
		 */
		static class myReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
			
			private IntWritable result = new IntWritable();
			
			int reduce_time = 1;
			
			@Override
			protected void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				System.out.println("这是第"+ reduce_time +"次reduce" + "key 为："+ key);
				System.out.println("Reduce阶段开始....");
				
				int sum = 0;
				for (IntWritable value : values) {
					sum += value.get();
				}
				result.set(sum);
				context.write(key, result);
				System.out.println(key +"->"+ result);
				reduce_time++;
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
		job.setJarByClass(MyMapReduce.class);
		
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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//==============shuffle=======================
		job.setCombinerClass(myCombiner.class);
		
		
		//==============shuffle=======================
		//设置运行的Reduce的相关参数
		job.setReducerClass(myReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean isSuccess =  job.waitForCompletion(true);
		
		System.out.println(
				"数据类型转换：" + "\n" + "Text文本类型，每次是一行数据"+ "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "\\" + "/" + "\n"
						+ "<<hadoop,sqoop,hive,hue,...>, 1>这类数据类似：Map<List<String>,IntWritable>" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "\\" + "/" + "\n"
						+ "<hadoop, <1,1,1,...>>这类数据类似：Map<Text, List<IntWritable>>" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "\\" + "/" + "\n"
						+ "<hadoop, 2>" + "\n"
						+ "<sqoop, 5>" + "\n"
						+ "<hive, 1>" + "\n"
						+ "<hue, 7>" + "\n"
						+ "<hive, 3>" + "\n"
						+ "<hadoop, 9>" + "\n"
						+ "这类数据类似：Map<Text, IntWritable>" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "||" + "\n"
						+ "\\" + "/" + "\n"
						+ "<hadoop, 11>" + "\n"
						+ "<sqoop, 5>" + "\n"
						+ "<hive, 4>" + "\n"
						+ "<hue, 7>" + "\n"
						+ "这类数据类似：Map<Text, IntWritable>"
				);
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://bigdata-study-104:8020/testdata/word.txt",
				"hdfs://bigdata-study-104:8020/testresult/output/"
		};

		try {
			int result = ToolRunner.run(conf, new MyMapReduce(), args);
			System.out.println(result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
