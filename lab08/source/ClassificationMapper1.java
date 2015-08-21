package classification;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassificationMapper1 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(",");
		int y_id = parts.length - 1;

		// Check if we need to process this row
		boolean flag = check(parts);
		if (flag) {
			for (int i = 0; i < parts.length - 1; ++i) {
				String emit_key = Helper.getAttr(i) + '=' + parts[i];
				String emit_value = parts[y_id];

				/**
				 * Emit: A=a_i y_i A: attribute name a_i: value of A on line i
				 * y_i: value of Y on line i
				 */
				context.write(new Text(emit_key), new Text(emit_value));
			}
		}
		cleanup(context);
	}

	boolean check(String[] parts) {
		for (int i = 0; i < parts.length - 1; ++i) {
			if (Helper.current_attr_val[i] == null) // first run
				return true;
			if (Helper.current_attr[i] == true) {
				return Helper.current_attr_val[i].equals(parts[i]);
			}
		}
		return false;
	}

}
