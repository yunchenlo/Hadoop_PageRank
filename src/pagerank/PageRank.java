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

import java.util.HashSet;
import java.util.Set;

import pagerank.BuildGraph;
import pagerank.BuildGraphMapper;
import pagerank.BuildGraphReducer;

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
		BuildGraph job1 = new BuildGraph();
		boolean complete = job1.BuildGraph(args);
		if (!complete) {
            System.exit(1);
        }
		PageRank.NODES.add("shit");
		PageRank.NODES.add("aka lol");
		System.out.println(NODES);
		// Job 2: Calculate
		//Calculate job2 = new Calculate();
		//complete = job2.Calculate(args);
		//if (!complete) {
        //    System.exit(1);
        //}
		// Job 3: Sort
		System.out.println("DONE!");
        System.exit(0);
	}

}
