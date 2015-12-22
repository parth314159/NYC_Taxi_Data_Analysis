package com.mapred.wei;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TipDriver {


  public static void main(String[] args) throws Exception {



	    /*

	     * Validate that two arguments were passed from the command line.

	     */

	    if (args.length != 2) {

	      System.out.printf("Usage: StubDriver <input dir> <output dir>\n");

	      System.exit(-1);

	    }


	    /*

	     * Instantiate a Job object for your job's configuration. 

	     */

	    Job job = new Job();

	    

	    /*

	     * Specify the jar file that contains your driver, mapper, and reducer.

	     * Hadoop will transfer this jar file to nodes in your cluster running 

	     * mapper and reducer tasks.

	     */

	    job.setJarByClass(TipDriver.class);

	    

	    /*

	     * Specify an easily-decipherable name for the job.

	     * This job name will appear in reports and logs.

	     */

	    job.setJobName("project 1");


	    /*

	     * TODO implement

	     */

	    

	    

	    FileInputFormat.setInputPaths(job,new Path(args[0]));

	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(TipMapper.class);

	job.setReducerClass(TipReducer.class);

	job.setMapOutputKeyClass(Text.class);

	job.setMapOutputValueClass(DoubleWritable.class);

	job.setOutputKeyClass(Text.class);

	job.setOutputValueClass(DoubleWritable.class);

	    /*

	     * Start the MapReduce job and wait for it to finish.

	     * If it finishes successfully, return 0. If not, return 1.

	     */

	    boolean success = job.waitForCompletion(true);

	    System.exit(success ? 0 : 1);
  }

}