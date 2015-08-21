package classification;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassificationMapper3 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace("\t", " ");
		String []parts = line.split(" ");
		int count = Integer.valueOf(parts[2]);
		int all = Integer.valueOf(parts[3]);
		
		double P = count * 1.0 / all;
		double H = P * Math.log(P) / Math.log(2);
		H = -H;
		
		String attr_name = parts[0].split("=")[0];
		String attr_value = parts[0].split("=")[1];
		String emit_key = attr_name;
		String emit_value = attr_value + " " + P + " " + H;
		
		context.write(new Text(emit_key), new Text(emit_value));
		
		// check and save if pure
		if(P > 0.66) {
			String attr = parts[0].split("=")[0];
			String attr_val = parts[0].split("=")[1];
			Helper.pure_map.put(Helper.getFullAttrName(attr, attr_val), parts[1]);
			
			System.out.printf("+++++Pure spotted! %s %s %.2f\n", 
					Helper.getFullAttrName(attr, attr_val), parts[1], P);
		}
		
		cleanup(context);
	}

}
