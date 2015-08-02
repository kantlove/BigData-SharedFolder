package k_means;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Center, IntWritable, Center, Vector> {

	List<Integer> children;

	public void reduce(Center key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		children = new ArrayList<Integer>();
		saveTemp(values);
		Center newCenter = calculateNewCenter();
		Vector vec = Vector.fromList(children); // vector contains children of
												// this centroid

//		System.out.println("old center: " + key.toString());
//		System.out.println("new center: " + newCenter.toString());
//		System.out.println("vector: " + vec.toString());

		context.write(newCenter, vec);
		cleanup(context);

		addNewCentersToSave(newCenter);
	}

	/**
	 * Memorize this center to save to file later
	 */
	private void addNewCentersToSave(Center newCenter) {
		KMeansConfig.newCentersToSave.append(newCenter.toString() + System.lineSeparator());
	}

	/**
	 * Because hadoop only allow one-time reading so I save the values
	 * to be able to read many times
	 */
	void saveTemp(Iterable<IntWritable> values) {
		for (IntWritable val : values)
			children.add(val.get());
	}

	Center calculateNewCenter() {
		Center newCenter = new Center(KMeansConfig.dimensions);
		int count = 0;
		for (int child : children) {
			Integer[] point = KMeansConfig.dataset.get(child);

			for (int i = 0; i < point.length; ++i) {
				newCenter.coor[i] += point[i];
			}

			count++;
		}

		// take average
		for (int i = 0; i < newCenter.coor.length; ++i) {
			newCenter.coor[i] = newCenter.coor[i] / count;
		}

		return newCenter;
	}

	
}
