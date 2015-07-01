package matrix_mul;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

// <in key class, in value class, out key class, out value class>
public class MatrixMulReducer extends
		Reducer<Text, IntArrayWritable, Text, Text> {
	
	private static final Text result_key = new Text("2");

	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Map<Integer, Long> products = new HashMap<Integer, Long>();
		
		while (values.iterator().hasNext()) {
			IntArrayWritable arr = values.iterator().next();
			
			System.out.println(arr.toString());
			
			Writable[] items = arr.get();
			//int id = ((IntWritable)items[0]).get();
			int j_value = ((IntWritable)items[1]).get();
			int val = ((IntWritable)items[2]).get();
			
			if(products.containsKey(j_value)) {
				products.put(j_value, products.get(j_value) * 1l * val);
			}
			else {
				products.put(j_value, val * 1l);
			}
		}
		
		long sum = 0;
		// now sum all the small product together -> cell_i_j
		for(Long p : products.values()) {
			sum += p;
		}
		
		Text result_val = new Text();
		result_val.set(String.format("%s %d", key, sum));
		
		context.write(result_key, result_val);
		
		cleanup(context);
	}

}
