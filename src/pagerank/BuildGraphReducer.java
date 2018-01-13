package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pagerank.PageRank;
import java.util.HashSet;

public class BuildGraphReducer extends Reducer<Text, Text, Text, Text>{
	private HashSet<String> NODES = new HashSet<String>();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// append the initial rank
		boolean first = true;
		if(key.toString().contains("\t")){
			NODES.add(key.toString().substring(1, key.toString().length()));
		}
		else {
			double init = 1.0/NODES.size();
			String links = init + "|";
			for (Text val:values) {
				if(NODES.contains(val.toString())){
					if(!first)
						links += ", ";
					links += val.toString();
					first = false;
				}
			}
			// write the result
	        context.write(key, new Text(links));
		}
       
	}

}