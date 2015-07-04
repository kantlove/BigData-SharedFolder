package general_matrix_mul;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GeneralMatrixMulMapper2 extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		context.write(new IntWritable(1), value);
		
		cleanup(context);
	}

}
