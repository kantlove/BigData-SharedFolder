package k_means;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Center, Text, Center, IntWritable> {
	
	List<Center> centers = KMeansConfig.centers;
	List<Integer> children;
	public void map(Center key, Text value, Context context)
			throws IOException, InterruptedException {

		children = new ArrayList<Integer>();
		
		readValue(value);
		
		for(Integer child : children) {
			Integer[] point = KMeansConfig.dataset.get(child);
			Center tmp = new Center(KMeansConfig.dimensions, point);
			float min = Integer.MAX_VALUE;
			Center nearest = centers.get(0);
			
			for(Center c : centers) {
				float d = tmp.distance(c);
				if(d < min) {
					min = d;
					nearest = c;
				}
			}
			context.write(nearest, new IntWritable(child));
		}
		cleanup(context);
	}

	/**
	 * Read the value of this key and parse it into
	 * a list of children
	 */
	void readValue(Text value) {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());

		while(tokenizer.hasMoreTokens()) {
			children.add(Integer.valueOf(tokenizer.nextToken()));
		}
	}
	
	
}
