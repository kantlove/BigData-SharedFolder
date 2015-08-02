package k_means;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class KMeansInputFormat extends FileInputFormat<Center, Text>{

@Override
public RecordReader<Center, Text> createRecordReader(InputSplit input,
		TaskAttemptContext arg1) throws IOException, InterruptedException {
    return new KMeansRecordReader();
}
}
