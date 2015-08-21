package classification;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassificationReducer3 extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		/**
		 * Key = attr value (ie a1, a2, ...)
		 * Value = pair of P and H
		 */
		Map<String, DoublePair> map = new HashMap<String, DoublePair>();
		
		for(Text val : values) {
			String s = val.toString();
			String []parts = s.split(" ");
			String attr_val = parts[1];
			double p = Double.valueOf(parts[1]);
			double h = Double.valueOf(parts[2]);
			DoublePair pair = new DoublePair(p, h);
			
			if(map.containsKey(attr_val))
				map.put(attr_val, map.get(attr_val).add(pair));
			else
				map.put(attr_val, pair);
		}
		
		double H_y_x = 0;
		for(DoublePair val : map.values()) {
			H_y_x += val.a * val.b;
		}
		
		String emit_value = "" + H_y_x;
		
		context.write(key, new Text(emit_value));
		cleanup(context);
	}
	
	class DoublePair {
		double a, b;
		public DoublePair(double a, double b) {
			this.a = a;
			this.b = b;
		}
		
		public DoublePair add(DoublePair other) {
			this.a += other.a;
			this.b += other.b;
			return this;
		}
	}

}
