package cn.itcast.mr.invertedindex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// 存储单词和文档名称
	private static Text keyInfo = new Text();
	// 存储词频，初始化为1
	private static final Text valueInfo = new Text("1");
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = StringUtils.split(line, " ");
		// 得到这行数据所在的文件切片
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		// 根据文件切片得到文件名
		String fileName = fileSplit.getPath().getName();
		for (String field : fields) {
			// key值由单词和文档名称组成，如“MapReduce: file1.txt”
			keyInfo.set(field + ":" + fileName);
			context.write(keyInfo, valueInfo);
		}
	}

}