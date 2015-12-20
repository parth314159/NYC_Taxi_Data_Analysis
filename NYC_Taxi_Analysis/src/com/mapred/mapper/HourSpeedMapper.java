package com.mapred.mapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.math3.exception.NotANumberException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HourSpeedMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>  {

	 @Override
	protected void map(LongWritable key, Text value,Context context)
					throws IOException, InterruptedException {
		 
		 String[] data_array = value.toString().split(",");

		 	//get user specified latitude and longitude for calculation
		 	Configuration conf = context.getConfiguration();
		 	String tlat = conf.get("lat");
			String tlng = conf.get("lng");
			
		 
			//set format for 11.1 meter 
			DecimalFormat locform = new DecimalFormat(" 00.000;-00.000");
			
			try {

				if (data_array.length >= 17) {
					
					//get drop off locaiton
					String dlat = locform.format(Double.parseDouble(data_array[10]));
					String dlng = locform.format(Double.parseDouble(data_array[9]));
			
				 
					
					//compare with the data
					if (dlat.trim().equals(tlat.trim()) && dlng.trim().equals(tlng.trim())) {
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

						//get pickup and dropoff timings and convert it to timestamp
						Date date1 = sdf.parse(data_array[1]);
						Date date2 = sdf.parse(data_array[2]);
					
						//get difference of pickup and dropoff time
						long diffInMillies = date2.getTime() - date1.getTime();
						double timeinhours = (double)diffInMillies/(double)(1000*60*60);
						
						//if difference zero throw exception otherwise data will be infinity
						if(timeinhours == 0)
						{
							throw new NotANumberException();
						}
						
						//calculate speed  by distance/time equation data_array[4] already have speed data 
						double speed = (double)(Double.parseDouble(data_array[4])/(double)timeinhours);
						
						//write the output 
	 				    context.write(new IntWritable(date2.getHours()), new DoubleWritable(speed));
					
					}
				}
			} catch (Exception e) {

			}

		 
	}
}