package k_means;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Helper {
	public static void readConfig(String[] args) {
		System.out.println("-----------------------");
		System.out.println("Reading config");
		System.out.println("-----------------------");
		
		KMeansConfig.dimensions = Integer.valueOf(args[0]);
		KMeansConfig.datasetPath = new Path("hdfs:/user/dev/" + args[1] + "/dataset.txt");
		KMeansConfig.centerPath = new Path("hdfs:/user/dev/" + args[2] + "/centers.txt");
		KMeansConfig.inputPath = new Path("hdfs:/user/dev/" + args[3] + "/kmeans_input.txt");
		KMeansConfig.outputPath = new Path("hdfs:/user/dev/" + args[4]);
		KMeansConfig.dataset = new ArrayList<Integer[]>();
		KMeansConfig.centers = new ArrayList<Center>();
		KMeansConfig.rounds = Integer.valueOf(args[5]);
		KMeansConfig.newCentersToSave = new StringBuffer();
	}

	public static void readDataset() {
		Path pt = KMeansConfig.datasetPath;
		System.out.println("****************************");
		System.out.println("Reading dataset");
		System.out.println("****************************");
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			while(line != null) {
				String[] parts = line.split(" ");
				Integer[] coor = new Integer[parts.length];
				
				for (int i = 0; i < parts.length; ++i) {
					coor[i] = Integer.valueOf(parts[i]);
				}
				System.out.println(Arrays.toString(coor));
				
				KMeansConfig.dataset.add(coor);
				
				line = br.readLine();
			}
			fs.close();
		} 
		catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void readCenters() {
		KMeansConfig.centers.clear();
		
		FileSystem fs = null;
		BufferedReader br = null;
		try {
			fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(KMeansConfig.centerPath)));
			String line = br.readLine();
			while(line != null) {
				String[] parts = line.split(" ");
				float[] coor = new float[parts.length];

				for (int i = 0; i < parts.length; ++i) {
					coor[i] = Float.valueOf(parts[i]);
				}

				Center key = new Center(coor.length, coor);
				KMeansConfig.centers.add(key);

				line = br.readLine();
			}			
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if(br != null) {
				try {
					br.close();
					fs.close();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void clearCenters() {
		FileSystem fs;
		Path file = KMeansConfig.centerPath;

		try {
			fs = FileSystem.get(new Configuration());
			System.out.println("--> Deleting center! " + file.toString());
			if(fs.exists(file)) {
				fs.delete(file, true);
				System.out.println("--> Delete successful!");
			}
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void clearInput() {
		FileSystem fs;
		Path file = KMeansConfig.inputPath;

		try {
			fs = FileSystem.get(new Configuration());
			System.out.println("--> Deleting input! " + file.toString());
			if(fs.exists(file)) {
				fs.delete(file, true);
				System.out.println("--> Delete successful!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void clearOutput() {
		FileSystem fs;
		Path file = KMeansConfig.outputPath;

		try {
			fs = FileSystem.get(new Configuration());
			System.out.println("Deleting output! " + file.toString());
			if(fs.exists(file)) {
				fs.delete(file, true);
				System.out.println("Delete successful!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void copyFromOutput() {
		System.out.println("-----------------------");
		System.out.println("Copy output to input \nfor the next round to use");
		System.out.println("-----------------------");
		FileSystem fs = null;
		BufferedReader br = null;
		try {
			fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(KMeansConfig.outputPath);
			StringBuilder content = new StringBuilder();
			for (int i = 0; i < status.length; i++) {
				br = new BufferedReader(new InputStreamReader(
						fs.open(status[i].getPath())));
				String line;
				line = br.readLine();
				
				while (line != null && line.length() > 0) {
					//System.out.println("--> Copied: " + line);
					
					content.append(line + System.lineSeparator());
					line = br.readLine();
				}
			}
			
			pasteToInput(content.toString());
		} 
		catch (Exception e) {
			System.out.println("File not found: " + KMeansConfig.outputPath.toString());
		}
	}
	
	public static void pasteToInput(String newInput) {
		FileSystem fs = null;
		BufferedWriter br = null;
		Path file = KMeansConfig.inputPath;
		try {
			fs = FileSystem.get(new Configuration());
			
			br = new BufferedWriter(new OutputStreamWriter(
					fs.create(file)));
			br.write(newInput);
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			if (br != null) {
				try {
					br.close();
					fs.close();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void saveNewCenters() {
		FileSystem fs = null;
		BufferedWriter br = null;
		Path file = KMeansConfig.centerPath;
		try {
			fs = FileSystem.get(new Configuration());
			br = new BufferedWriter(new OutputStreamWriter(
					fs.create(file)));
			br.write(KMeansConfig.newCentersToSave.toString());
//			System.out.println("-----------------------");
//			System.out.println("Saving new centers");
//			System.out.println("-----------------------");
//			System.out.println(KMeansConfig.newCentersToSave);
			
			KMeansConfig.newCentersToSave.setLength(0); // clear buffer
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			if (br != null) {
				try {
					br.close();
					fs.close();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
