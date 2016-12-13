package com.youku.data.LogLineCount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.youku.data.LogLineCount.mapreduce.LogTextMap;
import com.youku.data.LogLineCount.mapreduce.LogTextReduce;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        //System.out.println( "Hello World!" );
    	JobConf job = new JobConf(App.class);
    	job.setJobName("LogLineCount");
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	job.setMapperClass(LogTextMap.class);
    	job.setCombinerClass(LogTextReduce.class);
    	job.setReducerClass(LogTextReduce.class);

    	job.setInputFormat(TextInputFormat.class);
    	job.setOutputFormat(TextOutputFormat.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        JobClient.runJob(job);
    }
}
