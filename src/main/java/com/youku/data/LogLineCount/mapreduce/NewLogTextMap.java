package com.youku.data.LogLineCount.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewLogTextMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	private static final LongWritable one = new LongWritable(1);
	private static final Text lineKey = new Text("LineCount");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(lineKey, one);
	}

}
