package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Mapper produces two kinds of key value pairs
 * < "\t" + Title, space > <Title, Link> (Link need Capitalized first letter)
 */

public class BuildGraphMapper extends Mapper<Text, Text, Text, Text>{
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		/*  Match title pattern */ 
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher( unescapeXML(key.toString()) );
		String page = new String();
		while (titleMatcher.find()){
			page = titleMatcher.group();
		}
		
		/* Produce key(Title) */
		Text outkey = new Text(page.substring(7, page.length()-8));
		
		/* Send key for building Title Table */
		context.write(new Text( " " + page.substring(7, page.length()-8) ) , new Text(""));
		
		/*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher( unescapeXML(key.toString()) );
		
		while (linkMatcher.find()){
			String link = new String(linkMatcher.group());
			// potential problem
			if(link.substring(link.length(), link.length()) == "|" || link.substring(link.length(), link.length()) == "#") {
				String Caplink = new String(capitalizeFirstLetter(link.substring(2, link.length())));
				Text outvalue = new Text(Caplink);
				
				context.write(outkey, outvalue);
			}
			else {
				String Caplink = new String(capitalizeFirstLetter(link.substring(2, link.length()-2)));
				Text outvalue = new Text(Caplink);
				
				context.write(outkey, outvalue);
			}
		}
		context.write(outkey, new Text(""));
	}
	
	private String capitalizeFirstLetter(String original) {
	    if (original == null || original.length() == 0) {
	        return original;
	    }
	    return original.substring(0, 1).toUpperCase() + original.substring(1);
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