package k_means;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

class KMeansRecordReader extends RecordReader<Center, Text> {

	private LineReader lineReader;
	private Center key;
	private Text value;
	private long start = 0, end = 0, pos = 0;

	@Override
	public Center getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream filein = fs.open(split.getPath());

		lineReader = new LineReader(filein, conf);

		start = split.getStart();
		end = start + split.getLength();
		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// get the next line
		Text line = new Text();
		lineReader.readLine(line);

		if (line.toString() == null || line.toString() == "")
			return false;

		// parse the lineValue which is in the format:
		// x y z ... \t [num] [num] ....
		String[] pieces = line.toString().split("\t");
		String[] key_parts = pieces[0].split(" ");
		
		// try to parse floating point components of value
		float[] coor = new float[key_parts.length];
		try {
			for (int i = 0; i < coor.length; ++i) {
				coor[i] = Float.parseFloat(key_parts[i].trim());
			}
			// now that we know we'll succeed, overwrite the output objects
			key = new Center(coor.length, coor);
			value = new Text(pieces[1]);
		} 
		catch (NumberFormatException nfe) {
			// throw new
			// IOException("Error parsing floating point value in record");
			return false; // end of file or error reading. Anyway just ignore.
		}

		pos += line.getLength();
		return true;
	}

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}
}
