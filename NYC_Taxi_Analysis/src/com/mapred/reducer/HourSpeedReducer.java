package com.mapred.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HourSpeedReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{

	@Override
	protected void reduce(IntWritable Key, Iterable<DoubleWritable> Values, Context context)
					throws IOException, InterruptedException {
		
		double distance = 0d;
		int count=0;
		
		try
		{ 
		for (DoubleWritable value : Values) {
			distance += value.get();
			count++;
		}
		}
		catch(Exception e)
		{
			
		}
		
		context.write(Key, new DoubleWritable(distance/count));
		
	}
	
}
