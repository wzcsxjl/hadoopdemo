package cn.itcast.mr.dedup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DedupDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(DedupDriver.class);
		job.setMapperClass(DedupMapper.class);
		job.setReducerClass(DedupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, "D:\\Dedup\\input");
		// 指定处理完成之后的结果所保存的位置
		FileOutputFormat.setOutputPath(job, new Path("D:\\Dedup\\output"));
		job.waitForCompletion(true);
	}

}
