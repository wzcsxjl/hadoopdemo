package cn.itcast.mr.dedup;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DedupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private static Text field = new Text();
	
	// <0, 2018-3-1 a><11, 2018-3-2 b>
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		field = value;
		// NullWritable.get()方法设置空值
		// <2018-3-1 a, null><2018-3-2 b, null>
		context.write(field, NullWritable.get());
	}
}