package com.mapred.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapred.mapper.TimeDistanceMapper;
import com.mapred.reducer.HourSpeedReducer;

import org.apache.hadoop.mapreduce.Job;

public class HourDistanceeDriver {

	  public static void main(String[] args) throws Exception {
 
		    if (args.length != 2) {
		      System.out.printf(
		          "Usage: <Queryname> <input dir> <output dir>\n");
		      System.exit(-1);
		    }

		 
		    Job job = new Job();
		    
		    
		    job.setJarByClass(HourDistanceeDriver.class);
		     
		    job.setJobName("Time vs Distance");
 
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		    job.setMapperClass(TimeDistanceMapper.class);
		    job.setReducerClass(HourSpeedReducer.class);

		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(DoubleWritable.class);
		 
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(DoubleWritable.class);
		   
		    boolean success = job.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
		  }
	
	
}
