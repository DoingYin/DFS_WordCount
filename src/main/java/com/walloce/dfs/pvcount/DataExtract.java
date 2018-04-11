package com.walloce.pvcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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
import org.jboss.netty.util.internal.StringUtil;

import com.walloce.wordcount.MyMapReduce;

/**
 * 数据抽取，去掉多余的数据
* @ClassName: DataExtract  
* @Description: TODO(这里用一句话描述这个类的作用)  
* @author Walloce  
* @date 2018年3月28日
 */
public class DataExtract extends Configured implements Tool {

	static class ExtractMap extends Mapper<LongWritable, Text, PageView, NullWritable> {
		//设置统计量的初始值
		PageView pageView = new PageView();
		NullWritable init_val = NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			System.out.println("----------map阶段开始了--------------");
			
			//逐行处理数据
			String visitions[] = value.toString().split("\t");
			
			//日志信息过短，有异常的日志，过滤掉
			if(visitions.length<=31){
				context.getCounter("我的计数器", "日志信息长度小于31,不合格！").increment(1l);
				return;
			}

			//获取需要的关键字段的数据
			String browser_version = visitions[29];
			String province_id = visitions[23];
			String city_id = visitions[24];
			String visit_id = visitions[0];
			String visit_ip = visitions[13];
			String visit_platform = visitions[30];
			String visit_pre_url = visitions[2];
			String visit_session_id = visitions[10];
			String visit_url = visitions[1];
			String visit_user_guid = visitions[5];
			
			//必须是有效的访问
			if(StringUtils.isBlank(visit_id) || StringUtils.isBlank(visit_user_guid)) {
				context.getCounter("我的计数器", "访问必须有效");
				return;
			}
			
			//如果url为空，直接过滤
			if(StringUtils.isBlank(visit_url)){
				context.getCounter("我的计数器", "url信息为空").increment(1l);
				return;
			}
			
			//过滤ip为空的记录
			if (StringUtils.isBlank(visit_ip)) {
				context.getCounter("我的计数器", "访问的ip不能为空！");
				return;
			}
			
			//只记录在国内的访问量
			if (StringUtils.isBlank(province_id) || StringUtils.isBlank(city_id)
					|| StringUtils.isBlank(visit_session_id)) {
				context.getCounter("我的计数器", "访问的省份必须存在！");
				return;
			}
			
			/**
			 * 设置对象属性
			 */
			pageView.setBrowser_version(browser_version);
			pageView.setCity_id(city_id);
			pageView.setProvince_id(province_id);
			pageView.setVisit_id(visit_id);
			pageView.setVisit_ip(visit_ip);
			pageView.setVisit_platform(visit_platform);
			pageView.setVisit_pre_url(visit_pre_url);
			pageView.setVisit_session_id(visit_session_id);
			pageView.setVisit_url(visit_url);
			pageView.setVisit_user_guid(visit_user_guid);
			
			//输出数据
			context.write(pageView, init_val);
		}
	}
	
	static class ExtractReduce extends Reducer<PageView, NullWritable, PageView, NullWritable> {
		
		NullWritable result = NullWritable.get();
		@Override
		protected void reduce(PageView key, Iterable<NullWritable> values,
				Context context)
				throws IOException, InterruptedException {
			System.out.println("============reduce阶段开始了==============");
			
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
		job.setJarByClass(DataExtract.class);
		
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
		job.setMapperClass(ExtractMap.class);
		job.setMapOutputKeyClass(PageView.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//===========shuffle阶段===============
		
		
		//===========shuffle阶段===============
		
		/**
		 * 设置运行时的reduce参数
		 */
		job.setReducerClass(ExtractReduce.class);
		job.setOutputKeyClass(PageView.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(2);
		
		//判断运行是否成功
		boolean isSuccess =  job.waitForCompletion(true);
		
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://bigdata-study-104:8020/testdata/201508*", 
				"hdfs://bigdata-study-104:8020/testresult/extract1"};
		try {
			int result = ToolRunner.run(conf, new DataExtract(), args);
			System.out.println(result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
