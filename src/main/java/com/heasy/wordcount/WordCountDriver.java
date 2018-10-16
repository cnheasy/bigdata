package com.heasy.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设置环境变量
        System.setProperty("hadoop.home.dir", "/Volumes/heasy/02_大数据技术之Hadoop/02_大数据技术之Hadoop/2.资料/01_jar包/hadoop-2.7.2");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://hadoop14:9000");
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("yarn.resourcemanager.hostname","hadoop15");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
//        System.out.println(b?);
    }
}
