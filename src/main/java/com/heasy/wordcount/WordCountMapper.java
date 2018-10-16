package com.heasy.wordcount
        ;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入值:  行号,文本
 * 输出值:  文本,数量
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String string = value.toString();
        String replace = string.replace("[", " ")
                .replace("{", " ")
                .replace(":", " ")
                .replace(",", " ")
                .replace("}", " ")
                .replace("]", " ");
        String[] values = replace.split(" ");
        for (String v : values){
            context.write(new Text(v),new IntWritable(1));
        }
    }
}
