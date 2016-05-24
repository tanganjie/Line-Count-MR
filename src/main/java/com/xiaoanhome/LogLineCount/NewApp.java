package com.xiaoanhome.LogLineCount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.xiaoanhome.LogLineCount.mapreduce.NewLogTextMap;
import com.xiaoanhome.LogLineCount.mapreduce.NewLogTextReduce;

/**
 * Hello world!
 *
 */
public class NewApp 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	//JobConf job = new JobConf(NewApp.class);
    	Job job = Job.getInstance();
    	job.setJobName("LogLineCount");
    	
    	job.setJarByClass(NewApp.class);
    	
    	job.setMapperClass(NewLogTextMap.class);
    	job.setCombinerClass(NewLogTextReduce.class);
    	job.setReducerClass(NewLogTextReduce.class);

    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	
        job.waitForCompletion(true);
    }
}
