package com.mapred.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapred.mapper.PickupLocationMapper;
import com.mapred.mapper.TimeDistanceMapper;
import com.mapred.reducer.LocationReducer;
import com.mapred.reducer.HourSpeedReducer;

public class PickupLocationDriver {

 public static void main(String[] args) throws Exception {
	 
    if (args.length != 2) {
      System.out.printf(
          "Usage: <Queryname> <input dir> <output dir>\n");
      System.exit(-1);
    }

 
    Job job = new Job();
    
    
    job.setJarByClass(PickupLocationDriver.class);
     
    job.setJobName("Top Pickup Location");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
    job.setMapperClass(PickupLocationMapper.class);
    job.setReducerClass(LocationReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
   
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}