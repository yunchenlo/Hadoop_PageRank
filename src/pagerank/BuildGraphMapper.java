package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
/*
 * Mapper produces two kinds of key value pairs
 * < "\t" + Title, space > <Title, Link> (Link need Capitalized first letter)
 */

public class BuildGraphMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/*  Match title pattern */ 
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(value.toString() );
		if (titleMatcher.find()) {
			String title = titleMatcher.group(1);
			title = this.unescapeXML(title);
			// Send the title to partitioner
			for (int i = 0; i < PageRank.NumReducer; i++) {
				context.write(new Text(" "+Integer.toString(i)), new Text(title));
			}
		}
		
		titleMatcher = titlePattern.matcher(value.toString());
		while(titleMatcher.find()){
			String title = titleMatcher.group(1);
			// No need capitalizeFirstLetter
			title = this.unescapeXML(title);

			Text K = new Text("<title>" + title + "</title>");
			//  Match link pattern
			Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
			Matcher linkMatcher = linkPattern.matcher(value.toString());
			while (linkMatcher.find()) {
				String link = linkMatcher.group(1);
				link = this.unescapeXML(link);
				// Need capitalizeFirstLetter
				link = this.capitalizeFirstLetter(link);
				
				context.write(K, new Text( "<link>" + link + "</link>"));
			}
			// For dangling node
			Text V = new Text("");
			context.write(K, V);
		}
	}
	
	private String capitalizeFirstLetter(String original) {
		char firstChar = original.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( original.length() == 1 ){
                return original.toUpperCase();
            }
            else
                return original.substring(0, 1).toUpperCase() + original.substring(1);
        }
        else 
        	return original;
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