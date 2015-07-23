package association_rule;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	
	public static void main(String []args) 
		throws IOException, InterruptedException, ClassNotFoundException {
		/**
		 * Pass 1 (k = 1)
		 */
		Job job1 = Job.getInstance();
		
		job1.setJarByClass(Driver.class);
		job1.setJobName("Association rule");
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.setMapperClass(AssociationRuleMapper1.class);
		job1.setReducerClass(AssociationRuleReducer1.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		boolean success1 = job1.waitForCompletion(true);
		
		/**
		 * Pass 2 (k = 2)
		 */
		Job job2 = Job.getInstance();
		
		job2.setJarByClass(Driver.class);
		job2.setJobName("Association rule 2nd pass");
		
		FileInputFormat.setInputPaths(job2, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		job2.setMapperClass(AssociationRuleMapper2.class);
		job2.setReducerClass(AssociationRuleReducer2.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		boolean success2 = job2.waitForCompletion(true);
		
		/**
		 * Pass 3 (calculate Confidence)
		 */
		Job job3 = Job.getInstance();
		
		job3.setJarByClass(Driver.class);
		job3.setJobName("Association rule Confidence calculation");
		
		FileInputFormat.setInputPaths(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		job3.setMapperClass(AssociationRuleMapper3.class);
		job3.setReducerClass(AssociationRuleReducer3.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(FloatWritable.class);
		
		boolean success3 = job3.waitForCompletion(true);
		
		System.exit(success1 && success2 && success3 ? 0 : 1);
	}
}

