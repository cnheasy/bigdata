package com.heasy.wordcount
        ;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 输入值: 文本,数量
 * 输出值:  文本,数量
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        AtomicInteger sum = new AtomicInteger(0);

        values.forEach(value -> {
            sum.incrementAndGet();
        });

        context.write(key, new IntWritable(sum.get()));
    }
}
