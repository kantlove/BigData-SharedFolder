package classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassificationReducer2 extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		List<String> list = new ArrayList<String>();
		for(Text val : values) {
			String s = val.toString();
			String []parts = s.split(" ");
			sum += Integer.valueOf(parts[parts.length - 1]);
			
			list.add(s);
		}
		
		for(String s : list) {
			String emit_value = s + " " + sum;
			context.write(key, new Text(emit_value));
		}
		
		cleanup(context);
	}

}
