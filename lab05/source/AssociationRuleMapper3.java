package association_rule;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AssociationRuleMapper3 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String []parts = line.split("\\s+");
		if(line.contains("_")) {
			String out_key = parts[0].substring(0, parts[0].indexOf('_'));
			String out_val = parts[0] + '/' + parts[1];
			context.write(new Text(out_key), new Text(out_val));
		}
		else {
			String out_key = parts[0];
			String out_val = parts[1];
			context.write(new Text(out_key), new Text(out_val));
		}
		
		cleanup(context);
	}

}
