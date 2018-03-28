package com.walloce.pvcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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

import com.walloce.wordcount.MyMapReduce;

public class PvCountTest extends Configured implements Tool {

	static class pvMap extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		//设置统计量的初始值
		IntWritable init_Val = new IntWritable(1);
		LongWritable pro_id = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//逐行处理数据
			String visitions[] = value.toString().split("\t");
			
			//日志信息过短，有异常的日志，过滤掉
			if(visitions.length<=30){
				context.getCounter("我的计数器", "日志信息长度小于30,不合格！").increment(1l);
				return;
			}

			//分别获取用户进入的url和离开时的url
			String visit_inurl = visitions[1];
			String visit_outurl = visitions[2];
			
			//获取访问的ip
			String visit_ip = visitions[12];
			
			//获取访问的时间,暂定为开始访问时的时间
			String visit_time = visitions[14];
			
			String visit_sys = visitions[17];
			
			//访问时所在的省份
			String visit_province = visitions[19];
			
			//访问时所在的城市
			String visit_city = visitions[24];
			
			//城市对应的id
			String province_code = visitions[23];
			
			//如果url为空，直接过滤
			if(StringUtils.isBlank(visit_inurl)){
				context.getCounter("我的计数器", "url信息为空").increment(1l);
				return;
			}
			
			//过滤ip为空的记录
			if (StringUtils.isBlank(visit_ip)) {
				context.getCounter("我的计数器", "访问的ip不能为空！");
				return;
			}
			
			//只记录在国内的访问量
			if (StringUtils.isBlank(province_code) || StringUtils.isBlank(visit_province)
					|| StringUtils.isBlank(visit_city)) {
				context.getCounter("我的计数器", "访问的省份必须存在！");
				return;
			}
			
			//用户访问时的系统，排除其他访问情况
			if (StringUtils.isBlank(visit_sys)) {
				context.getCounter("我的计数器", "必须获取到访问的系统！");
				return;
			}
			
			if (StringUtils.isBlank(visit_time)) {
				context.getCounter("我的计数器", "访问时间不能少！");
				return;
			}
			
			/**
			 * 统计各个省份的访问情况
			 */
			pro_id.set(Integer.parseInt(province_code));
			context.write(pro_id, init_Val);
		}
	}
	
	static class pvReduce extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		
		IntWritable result = new IntWritable();
		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public int run(String[] args) throws Exception {
		
		//hadoop的八股文
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		
		/**
		 * 对job进行详细配置
		 */
		//当你本地运行，这个设置可以不写，不会报错
		//当提价到集群上面运行的时候，这个设置不写，会报类找不到的异常 
		job.setJarByClass(PvCountTest.class);
		
		//定义输入、输出路径
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		
		//输出前判断输出的路径是否存在
		FileSystem fs = outpath.getFileSystem(conf);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		/**
		 * 设置运行时的map参数
		 */
		job.setMapperClass(pvMap.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		/**
		 * 设置运行时的reduce参数
		 */
		job.setReducerClass(pvReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//判断运行是否成功
		boolean isSuccess =  job.waitForCompletion(true);
		
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://bigdata-study-104:8020/testdata/2015082818", 
				"hdfs://bigdata-study-104:8020/testresult/pvcount"};
		try {
			int result = ToolRunner.run(conf, new PvCountTest(), args);
			System.out.println(result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
