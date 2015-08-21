package classification;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Helper {
	
	static final String[] attr = {"buying", "maint", "doors", "persons", "lug_boot", "safety"};
	static final String[][] attr_values = {
		{"vhigh", "high", "med", "low"},
		{"vhigh", "high", "med", "low"},
		{"2", "3", "4", "5more"},
		{"2", "4", "more"},
		{"small", "med", "big"},
		{"low", "med", "high"}
	};
	static final boolean[] current_attr = new boolean[6];
	static final String[] current_attr_val = new String[6];
	// contains pure nodes
	// ex: A.a1 - y1 means A.a1 is pure and leads to y1
	static final Map<String, String> pure_map = new HashMap<String, String>();
	static Path h_x_y_file_path = null;
	
	public static String getAttr(int id) {
		return attr[id];
	}
	
	public static int getAttrId(String name) {
		for(int i = 0; i < attr.length; ++i) {
			if(attr[i].equals(name))
				return i;
		}
		return -1;
	}
	
	public static String getFullAttrName(int attrId, String value) {
		return "" + attr[attrId] + '.' + value;
	}
	
	public static String getFullAttrName(String attrName, String value) {
		return "" + attrName + '.' + value;
	}
	
	public static boolean isPure(int attrId, String value) {
		return pure_map.containsKey(getFullAttrName(attrId, value));
	}

	public static void clearFilesInPath(Path filePath) {
		FileSystem fs;
		Path file = filePath;

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
	
	public static int readNextSplitH_Y_X(Path filePath) {
		System.out.println("-----------------------");
		System.out.println("Reading hxy");
		System.out.println("-----------------------");
		FileSystem fs = null;
		BufferedReader br = null;
		try {
			fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(filePath);
			StringBuilder content = new StringBuilder();
			
			for (int i = 0; i < status.length; i++) {
				if(status[i].getPath().toString().contains("SUCCESS")) continue;
				br = new BufferedReader(new InputStreamReader(
						fs.open(status[i].getPath())));
				String line;
				line = br.readLine();
				System.out.println(status[i].getPath().toString());
				double chosen = 10000000;
				String chosen_attr = null;
				while (line != null && line.length() > 0) {
					line = line.replace("\t", " ");
					System.out.println(line);
					String[] parts = line.split(" ");
					String attr = parts[0];
					double H_y_x = Double.valueOf(parts[1]);
					
					if(H_y_x < chosen) {
						chosen = H_y_x;
						chosen_attr = attr;
					}
					
					line = br.readLine();
				}
				System.out.println(getAttrId(chosen_attr));
				return getAttrId(chosen_attr);
			}
		} 
		catch (Exception e) {
			System.out.println("File not found: " + h_x_y_file_path.toString());
		}
		return -1;
	}
}
