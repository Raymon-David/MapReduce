package com.zwg.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zwg
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    //每组调用一次，这一组数据特点：key相同，value可能有多个。
    @Override
    protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable i : arg1){
            sum = sum + i.get();
        }
        arg2.write(arg0, new IntWritable(sum) );
    }

}
