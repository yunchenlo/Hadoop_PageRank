package pagerank.job1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pagerank.PageRank;
import java.util.HashSet;

public class BuildGraphMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		pagerank.PageRank.NODES.add("fuck shit");
		/*  Match title pattern */ 
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher( unescapeXML(key.toString()) );
		String page = new String();
		while (titleMatcher.find()){
			page = titleMatcher.group();
			pagerank.PageRank.NODES.add(page);
			System.out.println(page);
		}
		
		Text outkey = new Text(page.substring(7, page.length()-8));
		
		/*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher( unescapeXML(key.toString()) );
		while (linkMatcher.find()){
			String link = new String(linkMatcher.group());
			if(link.substring(link.length()) == "|" || link.substring(link.length()) == "#") {
				Text outvalue = new Text(link.substring(2, link.length()-1));
				context.write(outkey, outvalue);
			}
			else {
				Text outvalue = new Text(link.substring(2, link.length()-2));
				context.write(outkey, outvalue);
			}
		}
	}
	
	private String unescapeXML(String input) {	
		input = input.replaceAll("&lt;", "<");
    	input = input.replaceAll("&gt;", ">");
    	input = input.replaceAll("&amp;", "&");
    	input = input.replaceAll("&quot;", "\"");
    	input = input.replaceAll("&apos;", "\'");
    	return input;
    }
}
