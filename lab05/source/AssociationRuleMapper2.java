package association_rule;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AssociationRuleMapper2 extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text id = new Text();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		if(line.contains("_")) { // this item is processed in previous pass
			String []parts = line.split("\\s+");
			String out_key = parts[0].substring(0, parts[0].length() - 1);
			int out_val = Integer.valueOf(parts[1]);
			context.write(new Text(out_key), new IntWritable(out_val));
		}
		else {
			// New items, we need to make pairs
			String []pool = line.split(" ");
			int n = pool.length;
			for(int i = 0; i < n; ++i) {
				for(int j = i + 1; j < n; ++j) {
					String pair = pool[i] + "_" + pool[j];
					id.set(pair);
					context.write(id, one);
				}
			}
		}
		
		cleanup(context);
	}

}
