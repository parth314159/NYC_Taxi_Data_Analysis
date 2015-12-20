package com.mapred.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocationReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text Key, Iterable<IntWritable> Values, Context context)
					throws IOException, InterruptedException {
		
		 
		int count=0;
		
		try
		{ 
		for (IntWritable value : Values) {
			count+=value.get();
		}
		}
		catch(Exception e)
		{
			
		}
		if(count>10)
		{
		context.write(Key, new IntWritable(count));
		}
	}
	
}
