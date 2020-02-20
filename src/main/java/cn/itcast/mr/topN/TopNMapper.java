package cn.itcast.mr.topN;

import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
	
	private TreeMap<Integer, String> repToRecordMap = new TreeMap<>();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String[] nums = line.split(" ");
		for (String num : nums) {
			// 读取每行数据写入TreeMap，超过5个就会移除最小的数值
			repToRecordMap.put(Integer.parseInt(num), "");
			if (repToRecordMap.size() > 5) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	}

	// 重写cleanup()方法，读取完所有文件行数据后，再输出到Reduce阶段
	@Override
	protected void cleanup(Context context) {
		for (Integer i : repToRecordMap.keySet()) {
			try {
				context.write(NullWritable.get(), new IntWritable(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}