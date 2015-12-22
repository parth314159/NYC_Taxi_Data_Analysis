package com.mapred.mapper;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TripMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {

	 @Override
	protected void map(LongWritable key, Text value,Context context)
					throws IOException, InterruptedException {
		 
		 
		 String[] data_array=value.toString().split(",");
		 
		 try{
		 if(data_array.length >= 17 )
		 { 
			 //getting pickup and dropoff lattiude and longitude
			 double plat=Double.parseDouble(data_array[6]);
			 double plng=Double.parseDouble(data_array[5]);
			 double dlat=Double.parseDouble(data_array[10]);
			 double dlng=Double.parseDouble(data_array[9]);
			 
			 //rounding it to 1 KM distance because of lots off data and output seem bigger 
			 DecimalFormat locform = new DecimalFormat(" 00.000;-00.000"); 
			 
			 //formating it to string
			 String splat = locform.format(plat);
			 String splng = locform.format(plng);
			 String sdlat = locform.format(dlat);
			 String sdlng = locform.format(dlng);
			 
			
		 //check if any of the lattitude or longitude is 0
	     //check if they are same becuase of GPS location problem 
		 //If not write to output file
	     if((plat != 0d || plng != 0d) && (dlat != 0d || dlng != 0d) && (!splat.equals(sdlat) || !splng.equals(sdlng)) )
	     {
		 context.write(new Text(splat+","+splng+":"+sdlat+","+sdlng), new IntWritable(1));
		 }
		 }
		 }
		 catch(Exception e)
		 {
			 
		 }
		 
	}
}
