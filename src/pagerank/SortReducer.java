package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;
import pagerank.SortPair;


public class SortReducer extends Reducer<SortPair, NullWritable, Text, DoubleWritable> {
	public void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key.getTitle(), new DoubleWritable(key.getRank()));
	}
}
