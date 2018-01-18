package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.counters.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.fs.FileSystem;

import pagerank.PageRank;

public class Sort {
	public static NumberFormat NF = new DecimalFormat("00");
	
	public Sort(){
		
	}
	public boolean Sort(String[] args) throws Exception {
		String in = new String();
		String out = new String();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sort");
		
        job.setJarByClass(Sort.class);
		
        // set the inputFormatClass <K, V>
        job.setInputFormatClass(TextInputFormat.class);

        // set the class of each stage in mapreduce
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // set the output class of Mapper and Reducer
        job.setMapOutputKeyClass(SortPair.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set the number of reducer
        job.setNumReduceTasks(1);

        // Change input output path
        in = args[1] + "/iter" + NF.format(PageRank.numIter%2);
        out = args[1] + "/ans";
        
        String kill = new String(args[1] + "/iter" + NF.format((PageRank.numIter+1)%2));
        
        // delete the output path if it exists
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(kill))) {
            fs.delete(new Path(kill), true);
        }
        
        // add input/output path
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        job.waitForCompletion(true);
        
		return true;
	}
}
