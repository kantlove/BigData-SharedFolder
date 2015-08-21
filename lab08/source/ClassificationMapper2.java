package classification;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassificationMapper2 extends
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace("\t", " ");
		String []parts = line.split(" ");
		String emit_key = parts[0];
		String emit_value = parts[1] + " " + parts[2];
		
		// Basically emit what we receive
		context.write(new Text(emit_key), new Text(emit_value));
		
		cleanup(context);
	}

}
