package cn.itcast.mr.invertedindex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// �洢���ʺ��ĵ�����
	private static Text keyInfo = new Text();
	// �洢��Ƶ����ʼ��Ϊ1
	private static final Text valueInfo = new Text("1");
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = StringUtils.split(line, " ");
		// �õ������������ڵ��ļ���Ƭ
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		// �����ļ���Ƭ�õ��ļ���
		String fileName = fileSplit.getPath().getName();
		for (String field : fields) {
			// keyֵ�ɵ��ʺ��ĵ�������ɣ��硰MapReduce: file1.txt��
			keyInfo.set(field + ":" + fileName);
			context.write(keyInfo, valueInfo);
		}
	}

}
