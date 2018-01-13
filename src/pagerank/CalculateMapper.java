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
 * Inputs : <Title, pagerank | N || linkA,linkB,...>
 * Mapper outputs two kinds of key value pairs
 * <"\s"+ dangling node, pagerank + N >, <link, pagerank/C> <title, | pagerank || links>
 */

public class CalculateMapper extends Mapper<Text, Text, Text, Text>{
	private Text space = new Text(" ");
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// init counter value
		context.getCounter(Status.error).setValue(0.0);
		
		// string variables
		String rank = new String();
		String links = new String();
		String N = new String();
		
		// find rank
		Pattern rankPattern = Pattern.compile("(.+?)|");
		Matcher rankMatcher = rankPattern.matcher( value.toString());
		while (rankMatcher.find()) {
			rank = rankMatcher.group().substring(0, rankMatcher.group().length()-1);
		}
		
		// find N
		Pattern nPattern = Pattern.compile("|(.+?)||");
		Matcher nMatcher = nPattern.matcher( value.toString());
		while (nMatcher.find()) {
			N = nMatcher.group().substring(1,nMatcher.group().length()-2);
		}
		
		// find link
		Pattern linkPattern = Pattern.compile("||(.+?)");
		Matcher linkMatcher = linkPattern.matcher( value.toString());
		while(linkMatcher.find()) {
			links = linkMatcher.group().substring(2);
		}
		
		String[] allOtherPages = links.split(",");
		int C = allOtherPages.length;
		
		if(C > 0){
			// write the PR(ti)/C
			for (String otherPage : allOtherPages) { 
	            Text pageRankWithTotalLinks = new Text(Double.parseDouble(rank)/C);
	            context.write(new Text(otherPage), pageRankWithTotalLinks); 
	        }
		}
		else {
			// write dangling number and N
			context.write( new Text(" " + key.toString()) , new Text(rank + "\t" + N));
		}
		// write original title link pair
		context.write(key, new Text("|" + rank + "||" + links));
		
	}
}
