package pagerank;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.HashSet;
import java.util.Set;

import pagerank.job1.BuildGraphMapper;
import pagerank.job1.BuildGraphReducer;

public class PageRank{
	
	public static HashSet<String> NODES = new HashSet<String>();
	
	public static Double DAMPING = 0.85;
	public static int NumReducer = 1;

	public static void main(String[] args) throws Exception {
		/*
		 * InputDir : args[0]
		 * OutDir : args[1]
		 */
		
		// Job 1: BuildGraph
		boolean complete = job1(args[0], args[1] + "/iter00");
		if (!complete) {
            System.exit(1);
        }
		PageRank.NODES.add("shit");
		PageRank.NODES.add("aka lol");
		System.out.println(NODES);
		// Job 2: Calculate
		
		// Job 3: Sort
		System.out.println("DONE!");
        System.exit(0);
	}
	
	public static boolean job1(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Job #1");
		job.setJarByClass(PageRank.class);
		
		// input / mapper
		FileInputFormat.addInputPath(job, new Path(in));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(BuildGraphMapper.class);
		
		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(BuildGraphReducer.class);
		
		return job.waitForCompletion(true);
		
	}
}
