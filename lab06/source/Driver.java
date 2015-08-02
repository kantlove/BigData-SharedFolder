package k_means;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Helper.readConfig(args);
		Helper.readDataset();
		
		boolean success = true;
		for(int i = 0; i < KMeansConfig.rounds; ++i) {
			System.out.println("========================================================");
			System.out.printf("Round %d\n", i + 1);
			System.out.println("========================================================");
			
			// setup
			if(i > 0) {
				Helper.clearInput();
				Helper.copyFromOutput(); // use output of the previous round as a new input
			}
			Helper.clearOutput();
			Helper.readCenters();
			Helper.clearCenters();
			
			Job job = Job.getInstance();

			job.setJarByClass(Driver.class);
			job.setJobName("K-Means");

			FileInputFormat.setInputPaths(job, new Path(args[3]));
			FileOutputFormat.setOutputPath(job, new Path(args[4]));

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);

			/* Must have if we want to parse custom input format */
			job.setInputFormatClass(KMeansInputFormat.class);

			/* Must have if use custom classes */
			job.setMapOutputKeyClass(Center.class);
			job.setMapOutputValueClass(IntWritable.class);

			/* Must have if use custom classes */
			job.setOutputKeyClass(Center.class);
			job.setOutputValueClass(Vector.class);

			success &= job.waitForCompletion(true);
			
			// cleanup
			Helper.saveNewCenters();
		}
		System.exit(success ? 0 : 1);
	}
}
