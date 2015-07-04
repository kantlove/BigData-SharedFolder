package general_matrix_mul;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {
	/* Must have! */
	public IntArrayWritable() {
		super(IntWritable.class, new IntWritable[0]);
	}
	
	public IntArrayWritable(IntWritable []values) {
		super(IntWritable.class, values);
	}
	
	@Override
	public Writable[] get() {
		return (Writable[])super.get();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Writable[] values = get();
		for(int i = 0; i < values.length; ++i) {
			sb.append(((IntWritable)values[i]).toString() + ' ');
		}
		
		return sb.toString();
	}
}
