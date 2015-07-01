package matrix_mul;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	
	public static void main(String []args) 
		throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = Job.getInstance();
		
		job.setJarByClass(Driver.class);
		job.setJobName("Matrix multiplication");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MatrixMulMapper.class);
		job.setReducerClass(MatrixMulReducer.class);
		
		/* Must have if use custom classes */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		
		/* Must have if use custom classes */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
