package classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// Preparation
		Path input_path1 = new Path(args[0]);
		Path output_path1 = new Path(args[1] + '1');
		Path input_path2 = output_path1;
		Path output_path2 = new Path(args[1] + '2');
		Path input_path3 = output_path2;
		Path output_path3 = new Path(args[1] + '3');
		Helper.h_x_y_file_path = new Path(args[1] + '3' + "/hxy.txt");
		Path tree_output_path = new Path(args[1]);

		Queue<AttrValue> nodes_to_process = new LinkedList<AttrValue>();
		boolean  success1 = true, success2 = true, success3 = true;
		do {
			if(nodes_to_process.size() > 0) {
				AttrValue cur = nodes_to_process.poll();
				Helper.current_attr_val[cur.attrId] = cur.attrVal;
			}
			
			Helper.clearFilesInPath(output_path1);
			Helper.clearFilesInPath(output_path2);
			Helper.clearFilesInPath(output_path3);
			
			/**
			 * Step 1
			 */
			Job job1 = Job.getInstance();

			job1.setJarByClass(Driver.class);
			job1.setJobName("Classification step 1");

			FileInputFormat.setInputPaths(job1, input_path1);
			FileOutputFormat.setOutputPath(job1, output_path1);

			job1.setMapperClass(ClassificationMapper1.class);
			job1.setReducerClass(ClassificationReducer1.class);

			/* Must have if use custom classes */
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			/* Must have if use custom classes */
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			success1 &= job1.waitForCompletion(true);

			/**
			 * Step 2
			 */
			Job job2 = Job.getInstance();

			job2.setJarByClass(Driver.class);
			job2.setJobName("Classification step 2");

			FileInputFormat.setInputPaths(job2, input_path2);
			FileOutputFormat.setOutputPath(job2, output_path2);

			job2.setMapperClass(ClassificationMapper2.class);
			job2.setReducerClass(ClassificationReducer2.class);

			/* Must have if use custom classes */
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			/* Must have if use custom classes */
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			success2 &= job2.waitForCompletion(true);

			/**
			 * Step 3
			 */
			Job job3 = Job.getInstance();

			job3.setJarByClass(Driver.class);
			job3.setJobName("Classification step 3");

			FileInputFormat.setInputPaths(job3, input_path3);
			FileOutputFormat.setOutputPath(job3, output_path3);

			job3.setMapperClass(ClassificationMapper3.class);
			job3.setReducerClass(ClassificationReducer3.class);

			/* Must have if use custom classes */
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);

			/* Must have if use custom classes */
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);

			success3 &= job3.waitForCompletion(true);

			/**
			 * Find the next attr to split
			 */
			// attr id of the next split
			int next_split = Helper.readNextSplitH_Y_X(output_path3);
			
			Helper.current_attr[next_split] = true; // mark as chosen
			
			String[] attr_values = Helper.attr_values[next_split];
			for (String val : attr_values) {
				if(!Helper.isPure(next_split, val)) {
					System.out.print("-----Enqueue node: ");
					System.out.println(Helper.getFullAttrName(next_split, val));
					nodes_to_process.add(new AttrValue(next_split, val));
				}
				else {
					System.out.print("*****Pure node: ");
					System.out.println(Helper.getFullAttrName(next_split, val));
				}
			}
			Helper.pure_map.clear();
		} 
		while(false); // run once
		//while (nodes_to_process.size() > 0);

		System.exit(success1 && success2 && success3 ? 0 : 1);
	}
	
	public static class AttrValue {
		int attrId;
		String attrVal;
		public AttrValue(int id, String val) {
			attrId = id;
			attrVal = val;
		}
	}
}
