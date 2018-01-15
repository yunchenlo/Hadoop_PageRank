package pagerank;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pagerank.Sort;
import pagerank.SortPair;

/*
 * Inputs : 
 * Mapper outputs key value pairs : 
 * 
 */

public class SortMapper extends Mapper<LongWritable, Text, SortPair, NullWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//string variables
		String rank = "";
		String links = "";
		
		// find title
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher( value.toString());
		titleMatcher.find();
		String title = titleMatcher.group(1);
		
		// find rank
		Pattern rankPattern = Pattern.compile("<PR>(.+?)</PR>");
		Matcher rankMatcher = rankPattern.matcher( value.toString());
		rankMatcher.find();
		rank = rankMatcher.group(1);
		
		context.write(new SortPair(new Text(title), Double.parseDouble(rank)), NullWritable.get());
		
		
	}
}
