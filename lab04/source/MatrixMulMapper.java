package matrix_mul;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMulMapper extends
		Mapper<LongWritable, Text, Text, IntArrayWritable> {
	
	private Text key_text = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(line.length() == 0 || line.charAt(0) == '#')
			return;
		String []parts = line.split(" ");
		
		int id = Integer.valueOf(parts[0]);
		int row = Integer.valueOf(parts[1]);
		int col = Integer.valueOf(parts[2]);
		int val = Integer.valueOf(parts[3]);
		
		int matrix_size = MatrixMulConfig.dimension; // our matrices size
		
		if(id == 0) {
			/*
			 * Output a pair (i,k), (M,j,v)
			 * i = row
			 * k = 0...n_col
			 * M = matrix id
			 * j = col
			 * v = value
			 */
			for(int k = 0; k < matrix_size; ++k) {
				line = String.format("%d %d", row, k);
				IntWritable[] values = {new IntWritable(id), 
						new IntWritable(col), 
						new IntWritable(val)};
				
				key_text.set(line);
				context.write(key_text, new IntArrayWritable(values));
			}
		}
		else if(id == 1) {
			/*
			 * Output a pair (i,k), (M,j,v)
			 * i = 0...n_row
			 * k = col
			 * M = matrix id
			 * j = row
			 * v = value
			 */
			for(int i = 0; i < matrix_size; ++i) {
				line = String.format("%d %d", i, col);
				IntWritable[] values = {new IntWritable(id), 
						new IntWritable(row), 
						new IntWritable(val)};
				
				key_text.set(line);
				context.write(key_text, new IntArrayWritable(values));
			}
		}
		
		cleanup(context);
	}

}
