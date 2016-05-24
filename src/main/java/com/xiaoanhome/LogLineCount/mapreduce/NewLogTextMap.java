package com.xiaoanhome.LogLineCount.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewLogTextMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private static final Text lineKey = new Text("LineCount");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!StringUtils.isEmpty(value.toString())) {
			context.write(lineKey, one);
		}
	}

}
