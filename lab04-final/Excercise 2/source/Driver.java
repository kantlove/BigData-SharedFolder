package general_matrix_mul;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String []args) 
			throws IOException, InterruptedException, ClassNotFoundException {
			/**
			 * Step 1
			 */
			Job job1 = Job.getInstance();
			
			job1.setJarByClass(Driver.class);
			job1.setJobName("General matrix mul step 1");
			
			FileInputFormat.setInputPaths(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			
			job1.setMapperClass(GeneralMatrixMulMapper.class);
			job1.setReducerClass(GeneralMatrixMulReducer.class);
			
			/* Mapper output. Must have if use custom classes */
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(IntArrayWritable.class);
			
			/* Reducer output. Must have if use custom classes */
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(LongWritable.class);
			
			boolean success_step1 = job1.waitForCompletion(true);
			
			/**
			 * Step 2
			 */
			Job job2 = Job.getInstance();
			
			job2.setJarByClass(Driver.class);
			job2.setJobName("General matrix mul step 2");
			
			FileInputFormat.setInputPaths(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
			job2.setMapperClass(GeneralMatrixMulMapper2.class);
			job2.setReducerClass(GeneralMatrixMulReducer2.class);
			
			/* Mapper output. Must have if use custom classes */
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			
			/* Reducer output. Must have if use custom classes */
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
			
			boolean success_step2 = job2.waitForCompletion(true);
			
			System.exit(success_step1 && success_step2 ? 0 : 1);
		}
}
