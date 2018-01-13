package pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import pagerank.PageRank;
import pagerank.BuildGraphMapper;
import pagerank.BuildGraphReducer;

public class BuildGraph {
	public BuildGraph(){
		
	}
	
	public boolean BuildGraph(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "BuildGraph");
        job.setJarByClass(BuildGraph.class);
		
        // set the inputFormatClass <K, V>
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // set the class of each stage in mapreduce
        job.setMapperClass(BuildGraphMapper.class);
        job.setPartitionerClass(BuildGraphPartitioner.class);
        job.setReducerClass(BuildGraphReducer.class);

        // set the output class of Mapper and Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the number of reducer
        job.setNumReduceTasks(PageRank.NumReducer);

        // add input/output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/iter00"));
        
        return job.waitForCompletion(true);
	}
}