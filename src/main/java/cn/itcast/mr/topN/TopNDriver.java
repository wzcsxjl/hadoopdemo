package cn.itcast.mr.topN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopNDriver.class);
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);
		// map�׶������key
		job.setMapOutputKeyClass(NullWritable.class);
		// map�׶������value
		job.setMapOutputValueClass(IntWritable.class);
		// reduce�׶������key
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\topN\\input"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\topN\\output"));
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

}
