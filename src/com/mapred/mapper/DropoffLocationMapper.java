package com.mapred.mapper;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DropoffLocationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] data_array = value.toString().split(",");

		try {
			if (data_array.length >= 17) // Ignore data if it has less then 17
											// attribute
			{
				double lat = Double.parseDouble(data_array[10]);
				double lng = Double.parseDouble(data_array[9]);

				// Rounding location for equator of 11.1 meter
				DecimalFormat locform = new DecimalFormat(" 00.000;-00.000");
				// DecimalFormat locform = new DecimalFormat(" 00.000;-00.000");
				// System.out.println(locform.format(lat)+","+locform.format(lng));
				// context.write(new Text(data_array[10]+","+data_array[9]), new
				// IntWritable(1));

				//Ignore data with lat=0 and long=0
				if (lat != 0d && lng != 0d)	
					context.write(new Text(locform.format(lat) + "," + locform.format(lng)), new IntWritable(1));
			}
		} catch (Exception e) {

		}

	}
}