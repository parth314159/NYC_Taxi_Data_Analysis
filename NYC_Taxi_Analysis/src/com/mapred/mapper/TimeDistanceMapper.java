package com.mapred.mapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeDistanceMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		 
		 String[] data_array=value.toString().split(",");
		 
		 try{
		 if(data_array.length >= 17 )
		 {
		 String pickup_time = data_array[1];
		 Double distance=Double.parseDouble(data_array[4]);
		 String hour= pickup_time.split(" ")[1].split(":")[0];
		 
		 
		 context.write(new IntWritable(Integer.parseInt(hour)+1), new DoubleWritable(distance));
		 }
		 }
		 catch(Exception e)
		 {
			 
		 }
	}
}