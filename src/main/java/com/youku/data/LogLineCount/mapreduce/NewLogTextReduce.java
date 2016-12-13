package com.youku.data.LogLineCount.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewLogTextReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private static final Text lineKey = new Text("LineCount");

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		Iterator<LongWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			count += iterator.next().get();
		}
		context.write(lineKey, new LongWritable(count));
	}
}
