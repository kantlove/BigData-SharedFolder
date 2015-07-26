package association_rule;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AssociationRuleReducer3 extends
		Reducer<Text, Text, Text, FloatWritable> {
	

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> store = new ArrayList<String>();
		for(Text val : values) {
			store.add(val.toString());
		}
		
		int countA = 0;
		ArrayList<Integer> countAB = new ArrayList<Integer>();
		ArrayList<String> idA = new ArrayList<String>();
		ArrayList<String> idB = new ArrayList<String>();
		
		for(String s : store) {
			String []tokens = s.split("\\/");
			if(tokens.length == 1) {
				countA = Integer.valueOf(tokens[0]);
				break;
			}
		}
		for(String s : store) {
			String []tokens = s.split("\\/");
			if(tokens.length > 1) {
				String []ids = tokens[0].split("_");
				idA.add(ids[0]);
				idB.add(ids[1]);
				countAB.add(Integer.valueOf(tokens[1]));
			}
		}
		
		int n = idA.size();
		for(int i = 0; i < n; ++i) {
			float conf = countAB.get(i) * 1.0f / countA;
			float minconf = AssociationRuleSettings.minconf;
			if(Float.compare(conf, minconf) >= 0) {
				String out_key = String.format("%s -> %s", idA.get(i), idB.get(i));
				context.write(new Text(out_key), new FloatWritable(conf));
			}
		}
		
		cleanup(context);
	}

}
