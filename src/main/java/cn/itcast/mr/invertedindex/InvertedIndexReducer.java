package cn.itcast.mr.invertedindex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	private static Text result = new Text();
	
	// ���룺<MapReduce, file3.txt: 2>
	// �����<MapReduce, file1.txt: 1; file2.txt: 1; file3.txt: 2; >
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// �����ĵ��б�
		String fileList = new String();
		for (Text value : values) {
			fileList += value.toString() + "; ";
		}
		result.set(fileList);
		context.write(key, result);
	}

}
