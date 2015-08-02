package k_means;

import java.util.List;

import org.apache.hadoop.fs.Path;

public class KMeansConfig {
	public static int rounds = 10; // How many rounds of clustering?
	
	public static int dimensions;
	public static Path centerPath;
	public static Path datasetPath;
	public static Path inputPath;
	public static Path outputPath;
	public static List<Integer[]> dataset;
	public static List<Center> centers;
	
	// hold the text that represents many new centers that need to be saved
	// also, StringBuffer is thread-safe
	public static StringBuffer newCentersToSave; 
}
