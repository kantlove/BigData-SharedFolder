package general_matrix_mul;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GeneralMatrixMulMapper extends
		Mapper<LongWritable, Text, IntWritable, IntArrayWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		if (!tokenizer.hasMoreTokens() || line.charAt(0) == '#')
			return;

		int matrix_id = Integer.valueOf(tokenizer.nextToken());
		int row = Integer.valueOf(tokenizer.nextToken());
		int col = 0;
		IntWritable emit_key = null;
		IntWritable[] values = null;
		
		while (tokenizer.hasMoreTokens()) {
			if (matrix_id == 0) {
				/**
				 * emit (col, {id, row, m_ij})
				 */
				emit_key = new IntWritable(col);
				int m_ij = Integer.valueOf(tokenizer.nextToken());
				values = createValue(matrix_id, row, m_ij);
			}
			else {
				/**
				 * emit (row, {id, col, m_ij})
				 */
				emit_key = new IntWritable(row);
				int m_ij = Integer.valueOf(tokenizer.nextToken());
				values = createValue(matrix_id, col, m_ij);
			}
			context.write(emit_key, new IntArrayWritable(values));
			
			col++;
		}

		cleanup(context);
	}

	public IntWritable[] createValue(int... args) {
		IntWritable[] results = new IntWritable[args.length];
		for (int i = 0; i < args.length; ++i) {
			results[i] = new IntWritable(args[i]);
		}
		return results;
	}

}
