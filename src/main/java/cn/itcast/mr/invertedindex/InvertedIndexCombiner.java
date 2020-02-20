package cn.itcast.mr.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
	
	private static Text info = new Text();
	
	// 输入：<MapReduce: file3.txt, {1,1}>
	// 输出：<MapReduce, file3.txt: 2>
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;	// 统计词频
		for (Text value : values) {
			sum += Integer.parseInt(value.toString());
		}
		int splitIndex = key.toString().indexOf(":");
		// 重新设置value值，由文档名和词频组成
		info.set(key.toString().substring(splitIndex + 1) + ": " + sum);
		// 重新设置key值为单词
		key.set(key.toString().substring(0, splitIndex));
		context.write(key, info);
	}

}