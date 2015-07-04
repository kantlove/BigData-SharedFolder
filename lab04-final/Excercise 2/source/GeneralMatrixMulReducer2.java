package general_matrix_mul;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GeneralMatrixMulReducer2 extends
		Reducer<IntWritable, Text, Text, LongWritable> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Long> results = new HashMap<String, Long>();

		while (values.iterator().hasNext()) {
			String line = values.iterator().next().toString();
			String []parts = line.split("\\s+"); // any space character: space, tab, ...
			String row = parts[0];
			String col = parts[1];
			long val_ik = Long.valueOf(parts[2]);
			
			String mapKey = row + " " + col;
			if(results.containsKey(mapKey)) {
				results.put(mapKey, results.get(mapKey) + val_ik);
			}
			else {
				results.put(mapKey, val_ik);
			}
		}
		
		for(Map.Entry<String, Long> entry : results.entrySet()) {
			context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
		}
		
		cleanup(context);
	}

}
