package association_rule;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AssociationRuleReducer2 extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable i : values) {
			sum += i.get();
		}
		
		if(sum >= AssociationRuleSettings.minsup_limit())
			context.write(key, new IntWritable(sum));
		
		cleanup(context);
	}

}
