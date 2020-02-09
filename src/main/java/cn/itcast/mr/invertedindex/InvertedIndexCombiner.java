package cn.itcast.mr.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
	
	private static Text info = new Text();
	
	// ���룺<MapReduce: file3.txt, {1,1}>
	// �����<MapReduce, file3.txt: 2>
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;	// ͳ�ƴ�Ƶ
		for (Text value : values) {
			sum += Integer.parseInt(value.toString());
		}
		int splitIndex = key.toString().indexOf(":");
		// ��������valueֵ�����ĵ����ʹ�Ƶ���
		info.set(key.toString().substring(splitIndex + 1) + ": " + sum);
		// ��������keyֵΪ����
		key.set(key.toString().substring(0, splitIndex));
		context.write(key, info);
	}

}
