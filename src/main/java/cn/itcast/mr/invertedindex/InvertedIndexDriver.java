package cn.itcast.mr.invertedindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(InvertedIndexDriver.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\InvertedIndex\\input"));
		// ָ���������֮��Ľ���������λ��
		FileOutputFormat.setOutputPath(job, new Path("D:\\InvertedIndex\\output"));
		// ��YARN��Ⱥ�ύ���job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

}
