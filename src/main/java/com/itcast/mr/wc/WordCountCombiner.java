package com.itcast.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// 局部汇总
		int count = 0;
		for (IntWritable v : values) {
			count += v.get();
		}
		context.write(key, new IntWritable(count));
	}

}