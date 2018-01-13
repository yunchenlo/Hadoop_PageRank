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
 * <"\s"+ dangling node, pagerank | N >, <link, pagerank/C> <title, | pagerank || links>
 */

public class CalculateMapper extends Mapper<Text, Text, Text, Text>{
	private Text space = new Text(" ");
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// init counter value
		context.getCounter(Status.error).setValue((long)0.0);
		
		// string variables
		String rank = new String();
		String links = new String();
		String N = new String();
		
		// find rank
		Pattern rankPattern = Pattern.compile("[0-9]*\\.?[0-9]+\\|");
		Matcher rankMatcher = rankPattern.matcher( value.toString());
		while (rankMatcher.find()) {
			rank = rankMatcher.group(1);
		}
		/*
		// find N
		Pattern nPattern = Pattern.compile("|(.+?)||");
		Matcher nMatcher = nPattern.matcher( value.toString());
		while (nMatcher.find()) {
			N = nMatcher.group(1);
		}
		
		// find link
		Pattern linkPattern = Pattern.compile("||(.+?)");
		Matcher linkMatcher = linkPattern.matcher( value.toString());
		while(linkMatcher.find()) {
			links = linkMatcher.group(1);
		}
		
		int C = 0;
		
		if(links != null){
			// cal and write the PR(ti)/C
			String[] allOtherPages = links.split(",");
			C = allOtherPages.length;
			double dangleAvg = Double.parseDouble(rank)/C;
			Text pageRankDivOutLinks = new Text(String.valueOf(dangleAvg));
			if(C > 0){
				for (String otherPage : allOtherPages) { 
		            context.write(new Text(otherPage), pageRankDivOutLinks); 
		        }
			}
		}
		else {
			// write dangling number and N
			context.write( new Text("|" + key.toString()) , new Text(rank + "|" + N));
		}
		// write original title link pair
		 
		 */
		context.write(key, new Text(rank));
		
	}
}
