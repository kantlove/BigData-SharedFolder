package vector_matrix_mul;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VectorMatrixMulMapper extends
		Mapper<LongWritable, Text, IntWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		if(!tokenizer.hasMoreElements() || line.charAt(0) == '#')
			return;
	
		int col = 0;
		int row = Integer.valueOf(tokenizer.nextToken());
		while(tokenizer.hasMoreElements()) {
			Long m_ij = Long.valueOf(tokenizer.nextToken());
			IntWritable emit_key = new IntWritable(col);
			LongWritable emit_val = new LongWritable(m_ij * SourceVector.vector.get(row));
			
			context.write(emit_key, emit_val);
			col++;
			System.out.printf("%d %d %d\n", row, m_ij, SourceVector.vector.get(row));
		}
		
		cleanup(context);
	}

}
