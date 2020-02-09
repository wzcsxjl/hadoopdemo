package cn.itcast.mr.dedup;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ����Map�׶δ����������ݣ�����Shuffle����ԭ����ֵkey��ͬ�����ݻᱻ�ϲ�����������ݾͲ�������ظ����ˡ�
 * @author wzcsx
 *
 */
public class DedupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	// <2018-3-1 a, null><2018-3-2 b, null><2018-3-3 c, null>
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}

}
