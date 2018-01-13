package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.counters.*;
import org.apache.hadoop.mapreduce.Counters;

import pagerank.PageRank;
import pagerank.CalculateMapper;
import pagerank.CalculateReducer;


public class Calculate {
	public static NumberFormat NF = new DecimalFormat("00");
		
	public Calculate(){
		
	}
	
	public boolean Calculate(String[] args) throws Exception {
		long ReturnErr = (long) 10.0;
		String in = new String();
		String out = new String();
		//while(ReturnErr > 0.001) {
		for(PageRank.numIter = 0; PageRank.numIter < 1; PageRank.numIter++) {
			Configuration conf = new Configuration();
			
			Job job = Job.getInstance(conf, "Calculate");
	        job.setJarByClass(Calculate.class);
			
	        // set the inputFormatClass <K, V>
	        job.setInputFormatClass(KeyValueTextInputFormat.class);
	
	        // set the class of each stage in mapreduce
	        job.setMapperClass(CalculateMapper.class);
	        job.setReducerClass(CalculateReducer.class);
	
	        // set the output class of Mapper and Reducer
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	
	        // set the number of reducer
	        job.setNumReduceTasks(PageRank.NumReducer);
	
	        // Change input output path
	        in = args[1] + "/iter" + NF.format(PageRank.numIter);
	        out = args[1] + "/iter" + NF.format(PageRank.numIter+1);
	        
	        // add input/output path
	        FileInputFormat.addInputPath(job, new Path(in));
	        FileOutputFormat.setOutputPath(job, new Path(out));
	        job.waitForCompletion(true);
	        
	        // update counter value
	        //Counters cn = job.getCounters();
	        //ReturnErr = cn.getCounter(Status.error);
	        ReturnErr = job.getCounters().findCounter(Status.error).getValue();
	        System.out.println("Error = " + ReturnErr);
	        
	        // update iteration number
	        //PageRank.numIter ++;
		}
        return true;
    }
}
