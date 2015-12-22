package com.mapred.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapred.mapper.HourSpeedMapper;
import com.mapred.mapper.TimeDistanceMapper;
import com.mapred.reducer.HourSpeedReducer;

import org.apache.hadoop.mapreduce.Job;

public class HourSpeedDriver {

	  public static void main(String[] args) throws Exception {
 
		    if (args.length != 4) {
		      System.out.printf(
		          "Usage: <Queryname> <input dir> <output dir> <Latitude> <logitude>\n");
		      System.exit(-1);
		    }

		 
		    Configuration conf = new Configuration();
		    conf.set("lat", args[2]);
		    conf.set("lng", args[3]);
		    Job job = new Job(conf);
		    
		    
		    job.setJarByClass(HourSpeedDriver.class);
		     
		    job.setJobName("Location-Hour vs Speed");
 
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		    job.setMapperClass(HourSpeedMapper.class);
		    job.setReducerClass(HourSpeedReducer.class);

		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(DoubleWritable.class);
		 
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(DoubleWritable.class);
		   
		    boolean success = job.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
		  }
	
	
}
