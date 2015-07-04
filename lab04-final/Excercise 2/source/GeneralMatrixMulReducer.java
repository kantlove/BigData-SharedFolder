package general_matrix_mul;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GeneralMatrixMulReducer extends
		Reducer<IntWritable, IntArrayWritable, Text, LongWritable> {

	public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		Map<Integer, Integer> valuesOfM = new HashMap<Integer, Integer>();
		Map<Integer, Integer> valuesOfN = new HashMap<Integer, Integer>();
		
		while (values.iterator().hasNext()) {
			IntArrayWritable received_values = values.iterator().next();
			Writable[] array = received_values.get();
			
			int matrix_id = ((IntWritable)array[0]).get();
			if(matrix_id == 0) {
				// save entry (i, m_ij)
				valuesOfM.put(((IntWritable)array[1]).get(), ((IntWritable)array[2]).get());
			}
			else {
				// save entry (k, n_jk)
				valuesOfN.put(((IntWritable)array[1]).get(), ((IntWritable)array[2]).get());
			}
		}
		
		/**
		 * For each pair (i, k)
		 * emit ({i, k}, m_ij * n_jk)
		 */
		for(Map.Entry<Integer, Integer> entryM : valuesOfM.entrySet()) {
			for(Map.Entry<Integer, Integer> entryN : valuesOfN.entrySet()) {
				String emit_key = "" + entryM.getKey() + " " + entryN.getKey();
				long emit_value = 1l * entryM.getValue() * entryN.getValue();
				
				context.write(new Text(emit_key), new LongWritable(emit_value));
			}
		}
		
		cleanup(context);
	}

}
