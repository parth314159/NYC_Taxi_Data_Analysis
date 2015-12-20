package com.mapred.wei;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class HourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override

	public void map(LongWritable key, Text value, Context context)

			throws IOException, InterruptedException {

		try {
			String line = value.toString();

			String totalS[] = line.split(",");

			String dateTime = totalS[1]; // yy-mm-dd h-m-s

			String date[] = dateTime.split(" ");

			String time[] = date[1].split(":");// y-m-d

			String hour = time[0];// m

			String min = time[1];

			int h = Integer.parseInt(hour);

			int m = Integer.parseInt(min);

			if (m >= 30) {

				h = (h + 1) % 24;

			}

			hour = String.valueOf(h);

			if (hour.length() == 1) {

				String tem = "0" + hour;

				hour = tem;

			}

			context.write(new Text(hour), new IntWritable(1));
		} catch (Exception e) {

		}

	}

}
