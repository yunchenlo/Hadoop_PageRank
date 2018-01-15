package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;

import java.util.HashMap;

/*
 * Input : <"\s"+ dangling node, pagerank + N >, <link, pagerank/C> <title, links>
 * Output : <Title, newPR | N || linkA,linkB,...>
 */

public class CalculateReducer extends Reducer<Text, Text, Text, Text>{
	private HashMap<String, String> TMap = new HashMap<String, String>();
	private HashMap<String, String> DangMap = new HashMap<String, String>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String links = "";
		double sumShareOtherPageRanks = 0.0;
		double oldRank = 0.0;
		double DanglePageRank = 0.0;
		int N = 0;
		double dangleDivN = 0.0;
		double err = 0.0;
		
		if(key.toString().startsWith("\t")){
			// count (sum pr of dangling nodes)/N
			int num = 0;
			for(Text val: values) {
				String[] split = val.toString().split("#");
				DanglePageRank =  Double.parseDouble(split[0]);
				N = Integer.parseInt(split[1]);
				dangleDivN += DanglePageRank/N;
				num ++;
			}
			DangMap.put("IN", String.valueOf(dangleDivN));
			context.getCounter(Status.numDangle).increment(num);
		}
		else if(key.toString().startsWith(" ")) {
			// count out_pr/C
			for (Text val: values) {
				sumShareOtherPageRanks += Double.parseDouble(val.toString());
			}
			context.getCounter(Status.numNormal).increment(1);
			Pattern pagePattern = Pattern.compile("</No>(.+?)<end>");
			Matcher pageMatcher = pagePattern.matcher( key.toString());
			if(pageMatcher.find())
				TMap.put(pageMatcher.group(1), String.valueOf(sumShareOtherPageRanks));
		}
		else {
			// pass the value
			for (Text val: values) {
				Pattern rankPattern = Pattern.compile("<PR>(.+?)</PR>");
				Matcher rankMatcher = rankPattern.matcher( val.toString());
				rankMatcher.find();
				oldRank = Double.parseDouble(rankMatcher.group(1).toString());
				
				Pattern nPattern = Pattern.compile("<N>(.+?)</N>");
				Matcher nMatcher = nPattern.matcher( val.toString());
				nMatcher.find();
				N = Integer.parseInt(nMatcher.group(1).toString());
				
				Pattern linkPattern = Pattern.compile("<content>(.+?)</content>");
				Matcher linkMatcher = linkPattern.matcher( val.toString());
				if(linkMatcher.find())
					links = linkMatcher.group(1);
				
				// get map value
				String title = new String(key.toString()); 
				if(TMap.containsKey(title)){
					String sumString = TMap.get(title);
					sumShareOtherPageRanks = Double.parseDouble(sumString);
				}
				String dangle = DangMap.get("IN");
				dangleDivN = Double.parseDouble(dangle);
				
				double newRank = 0.15 / N + 0.85 * sumShareOtherPageRanks + 0.85 * dangleDivN;
				err = Math.abs(newRank - oldRank);
				long error = (long)(1E18*err);
				context.getCounter(Status.error).increment(error);
				
				Text newK = new Text("<title>" + key.toString() + "</title>");
				String valueString = "<PR>" + newRank + "</PR>" + "<N>" + N + "</N>";
				valueString += "<content>" + links + "</content>";
				Text newV = new Text(valueString);
				
				context.write(newK,newV);
			}
		}
	}
}
