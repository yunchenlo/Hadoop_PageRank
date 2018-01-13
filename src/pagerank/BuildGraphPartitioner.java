package pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BuildGraphPartitioner extends Partitioner<Text, Text>{
	@Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
		if (key.toString().charAt(0) == ' ') {
			return Integer.parseInt(key.toString().substring(1, key.toString().length()));
		}
		else{
			return (int)(key.toString().charAt(0))%numReduceTasks;
		}
	}
}
