package com.mapred.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapred.mapper.PickupLocationMapper;
import com.mapred.mapper.TripMapper;
import com.mapred.reducer.LocationReducer;
import com.mapred.reducer.TripReducer;

public class TripDriver {
	public static void main(String[] args) throws Exception {
		 
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: <Queryname> <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	 
	    Job job = new Job();
	    
	    
	    job.setJarByClass(TripDriver.class);
	     
	    job.setJobName("Top trips");

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 
	    job.setMapperClass(TripMapper.class);
	    job.setReducerClass(TripReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	   
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
}
