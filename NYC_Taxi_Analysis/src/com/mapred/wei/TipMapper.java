package com.mapred.wei;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class TipMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {


  @Override

  public void map(LongWritable key, Text value, Context context)

throws IOException,InterruptedException {
	  try {

String line= value.toString();

String totalS[]=line.split(",");
double totalamount=Double.parseDouble(totalS[17]);

double lon=Double.parseDouble(totalS[5]);

double longtitude=(double)(Math.round(lon*1000)/1000.0);//round  3 decimal

double lat=Double.parseDouble(totalS[6]);

double lattitude=(double)(Math.round(lat*1000)/1000.0);//round  3 decimal

String l=String.valueOf(longtitude);

String la=String.valueOf(lattitude);

String location=la+","+l;

double tip=Double.parseDouble(totalS[15]);
double avgTip=tip/totalamount;

context.write(new Text(location), new DoubleWritable(avgTip*100));
	  }
	  catch(Exception e)
	  {}

  }
}