package cn.itcast.mr.topN;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	
	// ����TreeMap����ʵ���Զ��嵹���������
	private TreeMap<Integer, String> repToRecordMap = new TreeMap<>(new Comparator<Integer>() {
		
		// int compare(object o1, object o2)����һ���������͵����ͣ�˭��˭�ź���
		// ���ظ�����ʾ��o1С��o2
		// ����0��ʾ ��o1��o2���
		// ����������ʾ��o1����o2

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
