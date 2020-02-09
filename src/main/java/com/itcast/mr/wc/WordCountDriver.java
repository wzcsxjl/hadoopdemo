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
		// ͨ��Job����װ����MR�������Ϣ
		Configuration conf = new Configuration();
		// ����MR����ģʽ��ʹ��local��ʾ����ģʽ������ʡ��
		conf.set("mapreduce.framework", "local");
		Job wcJob = Job.getInstance(conf);
		// ָ��MR Job jar����������
		wcJob.setJarByClass(WordCountDriver.class);
		// ָ������MR���е�Mapper��Combiner��Reducer��
		wcJob.setMapperClass(WordCountMapper.class);
		wcJob.setCombinerClass(WordCountCombiner.class);
		wcJob.setReducerClass(WordCountReducer.class);
		// ����ҵ���߼�Mapper������key��value����������
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(IntWritable.class);
		// ����ҵ���߼�Reducer������key��value����������
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		// ʹ�ñ���ģʽָ��Ҫ������������ڵ�λ��
		FileInputFormat.setInputPaths(wcJob, "D:/mr/input");
		// ʹ�ñ���ģʽָ��������֮��Ľ���������λ��
		FileOutputFormat.setOutputPath(wcJob, new Path("D:/mr/output"));
		// �ύ�����Ҽ�ش�ӡ����ִ�����
		boolean res = wcJob.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

}
