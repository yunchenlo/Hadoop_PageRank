package pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CalculatePartitioner extends Partitioner<Text, Text>{
	@Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
		if ( key.toString().length() > 0 && key.toString().charAt(0) == ' ') {
			Pattern reducePattern = Pattern.compile("<No>(.+?)</No>");
			Matcher reduceMatcher = reducePattern.matcher( key.toString());
			reduceMatcher.find();
			return Integer.parseInt(reduceMatcher.group(1));
		}
		else if(key.toString().length() > 0 && key.toString().charAt(0) == '\t') {
			return Integer.parseInt(key.toString().substring(1, key.toString().length()));
		}
		else if (key.toString().length() > 0){
			return (int)(key.toString().charAt(0))%numReduceTasks;
		}
		else {
			return 0;
		}
	}
}
