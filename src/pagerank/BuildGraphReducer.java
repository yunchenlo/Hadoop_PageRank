package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.HashSet;

/*
 * Input : < "\t" + Title, space > <Title, Link> (Link need Capitalized first letter)
 * Output : <Title, 1/N | N || linkA<tab>linkB<tab>...>
 */

public class BuildGraphReducer extends Reducer<Text, Text, Text, Text>{
	private HashSet<String> NODES = new HashSet<String>();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// append the initial rank
		boolean first = true;
		if(key.toString().startsWith(" ")){
			for(Text val:values){
				NODES.add(val.toString());
			}
		}	
		else {
			double init = 1.0/NODES.size();
			String links = "<PR>" + String.valueOf(init) + "</PR>";
			links += "<N>" + NODES.size() + "</N>" + "<content>";
			for (Text val:values) {
				Pattern linkPattern = Pattern.compile("<link>(.+?)</link>");
				Matcher linkMatcher = linkPattern.matcher(val.toString());
				while(linkMatcher.find()){
					String link = linkMatcher.group(1);
					if(NODES.contains(link) && !val.toString().equals("")){
						if(!first)
							links += "<tab>";
						links += link;
						first = false;
					}
				}
			}
			links += "</content>";
			// write the result
	        context.write(key, new Text(links));
		}
       
	}

}