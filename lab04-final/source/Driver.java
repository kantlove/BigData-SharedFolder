package vector_matrix_mul;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String []args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		Job job = Job.getInstance();
		
		job.setJarByClass(Driver.class);
		job.setJobName("Vector x Matrix");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/* Read the input vector separately */
		readVector(new Path(args[2]));
		
		job.setMapperClass(VectorMatrixMulMapper.class);
		job.setReducerClass(VectorMatrixMulReducer.class);
		
		/* Mapper output. Must have if use custom classes */
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		/* Reducer output. Must have if use custom classes */
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
	
	public static void readVector(Path path) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line = br.readLine();
			StringTokenizer tokenizer = new StringTokenizer(line);

			SourceVector.vector.clear();
			while (tokenizer.hasMoreTokens()) {
				SourceVector.vector.add(Integer.valueOf(tokenizer.nextToken()));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
