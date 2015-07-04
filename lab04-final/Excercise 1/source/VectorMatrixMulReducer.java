package vector_matrix_mul;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VectorMatrixMulReducer extends
		Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		while (values.iterator().hasNext()) {
			sum += values.iterator().next().get();
		}
		
		context.write(key, new LongWritable(sum));
		cleanup(context);
	}

}
