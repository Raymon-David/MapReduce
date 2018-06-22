package com.zwg.mapreduce.recommendfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.System.out;

public class RunJob {

    public static void main(String[] args) {

        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("mapred.jar", "E:\\IDEA\\workspace\\mapreduce\\out\\artifacts\\mapreduce\\mapreduce.jar");

        run1(configuration);

    }

    public static void run1(Configuration configuration){

        try {
            //URI uri = new URI(hdfsUrl.trim());
            FileSystem fs = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);

            job.setJarByClass(com.zwg.mapreduce.wordcount.RunJob.class);
            job.setJobName("recommendfriends");
            job.setMapperClass(RecommendFriendsMapper.class);
            job.setReducerClass(RecommendFriendsReducer.class);
            job.setMapOutputKeyClass(FridenOfFriend.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(IntWritable.class);

            //input和output的路径是指在HDFS上的路径，不是操作系统上的路径
            FileInputFormat.addInputPath(job, new Path("/usr/hadoop/input/recommenfriends"));

            Path outpath = new Path("/usr/hadoop/output/recommenfriends");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if(f){
                out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
