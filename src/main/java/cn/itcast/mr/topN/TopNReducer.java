package cn.itcast.mr.topN;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	
	// 创建TreeMap，并实现自定义倒序排序规则
	private TreeMap<Integer, String> repToRecordMap = new TreeMap<>(new Comparator<Integer>() {
		
		// int compare(object o1, object o2)返回一个基本类型的整型，谁大谁排后面
		// 返回负数表示：o1小于o2
		// 返回0表示 ：o1和o2相等
		// 返回正数表示：o1大于o2

		@Override
		public int compare(Integer a, Integer b) {
			return b - a;
		}
	});
	
	public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable value : values) {
			repToRecordMap.put(value.get(), " ");
			if (repToRecordMap.size() > 5) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		for (Integer i : repToRecordMap.keySet()) {
			context.write(NullWritable.get(), new IntWritable(i));
		}
	}

}