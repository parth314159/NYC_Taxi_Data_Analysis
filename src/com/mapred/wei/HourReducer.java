package com.mapred.wei;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class HourReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>

{
	
	public  double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}

	@Override

	public void reduce(Text key, Iterable<IntWritable> values, Context context)

			throws IOException, InterruptedException {

		Double wordCount = 0d;

		try {
			for (IntWritable value : values) {

				wordCount += value.get();

			}

			context.write(key, new DoubleWritable(round(wordCount/180,2)));
		} catch (Exception e) {

		}

	}

}