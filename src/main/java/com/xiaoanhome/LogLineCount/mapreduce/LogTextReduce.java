package com.xiaoanhome.LogLineCount.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LogTextReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	private Text lineKey = new Text("LineCount");
	
	public void reduce(Text key, Iterator<IntWritable> values, 
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		int count = 0;
		while(values.hasNext()){
			count += values.next().get();
		}
		output.collect(lineKey, new IntWritable(count));
	}

}
