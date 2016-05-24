package com.xiaoanhome.LogLineCount.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewLogTextReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static final Text lineKey = new Text("LineCount");

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			count += iterator.next().get();
		}
		context.write(lineKey, new IntWritable(count));
	}
}
