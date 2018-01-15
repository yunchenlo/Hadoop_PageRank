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
import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.util.Arrays;

public class PageRank{
	
	public static Double DAMPING = 0.85;
	public static int NumReducer = 1;
	public static int numIter = 0;

	public static void main(String[] args) throws Exception {
		/*
		 * InputFile : args[0]
		 * OutDir : args[1]
		 */
		long time1= System.nanoTime();
		
		// Job 1: BuildGraph
		BuildGraph job1 = new BuildGraph();
		boolean complete = job1.BuildGraph(args);
		if (!complete) {
            System.exit(1);
        }
		
		// Job 2: Calculate
		
		Calculate job2 = new Calculate();
		complete = job2.Calculate(args);
		if (!complete) {
            System.exit(1);
        }
        
		
		// Job 3: Sort
		Sort job3 = new Sort();
		complete = job3.Sort(args);
		if (!complete) {
            System.exit(1);
        }
		
		long time2 = System.nanoTime();
		long timeSpent = time2-time1;
		System.out.println("DONE with "+ NumReducer + " reducers, time is:" + timeSpent/1E9 +"s");
		System.out.println("iter/err info:");
		System.out.println(Arrays.asList(Calculate.errMap));
        System.exit(0);
	}

}