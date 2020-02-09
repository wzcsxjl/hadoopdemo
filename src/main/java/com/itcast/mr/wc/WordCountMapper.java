package com.itcast.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// ���մ��������һ���ı�������������ת��ΪString����
		String line = value.toString();
		// ���������ݰ��շָ����и�
		String[] words = line.split(" ");
		// �������飬ÿ����һ�����ʾͱ��һ������1���磺<����, 1>
		for (String word : words) {
			// ʹ��context����Map�׶δ�������ݷ��͸�Reduce�׶���Ϊ��������
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
