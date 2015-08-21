package classification;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassificationReducer1 extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Integer> sum_map = new TreeMap<String, Integer>();
		
		for(Text val : values) {
			String s = val.toString();
			if(sum_map.containsKey(s)) {
				sum_map.put(s, sum_map.get(s) + 1);
			}
			else {
				sum_map.put(s, 1);
			}
		}
		
		for(Entry<String, Integer> entry : sum_map.entrySet()) {
			String emit_value = entry.getKey() + ' ' + entry.getValue();
			context.write(key, new Text(emit_value));
		}
		
		cleanup(context);
	}

}
