package pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import pagerank.Calculate;

/*
 * Inputs : <Title, pagerank | N || linkA<tab>linkB,...>
 * Mapper outputs two kinds of key value pairs
 * <"\s"+ dangling node, pagerank # N >, <link, "\s" + pagerank/C> <title, # pagerank ## links>
 */

public class CalculateMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// string variables
		String rank = "";
		String links = "";
		String N = "";
		
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
		
		// find N
		Pattern nPattern = Pattern.compile("<N>(.+?)</N>");
		Matcher nMatcher = nPattern.matcher( value.toString());
		nMatcher.find();
		N = nMatcher.group(1);
		
		// find links
		Pattern linkPattern = Pattern.compile("<content>(.+?)</content>");
		Matcher linkMatcher = linkPattern.matcher( value.toString());
		if(linkMatcher.find())
			links = linkMatcher.group(1);
		
		// dangling pattern
		Pattern danglePattern = Pattern.compile("<content></content>");
		Matcher dangleMatcher = danglePattern.matcher( value.toString());

		int C = 0;
		if(!dangleMatcher.find()){
			// cal and write the PR(ti)/C
			String[] allOtherPages = links.split("<tab>");
			C = allOtherPages.length;
			double outAvg = Double.parseDouble(rank)/C;
			Text prDivOutLinks = new Text(String.valueOf(outAvg));
			for (String otherPage : allOtherPages) {
				context.write(new Text(" " + otherPage), prDivOutLinks); 
	        }

		}
		else {
			// write dangling number and N
			context.write( new Text("\t") , new Text(rank + "#" + N));
		}
		// write original title link pair
		context.write(new Text(title), value);
	}
}
