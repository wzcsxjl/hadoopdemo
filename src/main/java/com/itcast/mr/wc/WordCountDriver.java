package com.itcast.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
		// 通过Job来封装本次MR的相关信息
		Configuration conf = new Configuration();
		// 配置MR运行模式，使用local表示本地模式，可以省略
		conf.set("mapreduce.framework", "local");
		Job wcJob = Job.getInstance(conf);
		// 指定MR Job jar包运行主类
		wcJob.setJarByClass(WordCountDriver.class);
		// 指定本次MR所有的Mapper、Combiner、Reducer类
		wcJob.setMapperClass(WordCountMapper.class);
		wcJob.setCombinerClass(WordCountCombiner.class);
		wcJob.setReducerClass(WordCountReducer.class);
		// 设置业务逻辑Mapper类的输出key和value的数据类型
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(IntWritable.class);
		// 设置业务逻辑Reducer类的输出key和value的数据类型
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		// 使用本地模式指定要处理的数据所在的位置
		FileInputFormat.setInputPaths(wcJob, "D:/mr/input");
		// 使用本地模式指定处理完之后的结果所保存的位置
		FileOutputFormat.setOutputPath(wcJob, new Path("D:/mr/output"));
		// 提交程序并且监控打印程序执行情况
		boolean res = wcJob.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

}