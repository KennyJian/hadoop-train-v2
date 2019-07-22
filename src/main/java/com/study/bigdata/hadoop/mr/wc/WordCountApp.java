package com.study.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 黄思佳
 * @Date: 2019/7/22 10:26
 */
public class WordCountApp {

    public static void main(String[] args) {
        try {

            System.setProperty("HADOOP_USER_NAME", "hadoop");

            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://192.168.147.3:8020");

            //创建job
            Job job = Job.getInstance(configuration);

            //设置job运行的主类
            job.setJarByClass(WordCountApp.class);

            //设置运行的mapper类
            job.setMapperClass(WordCountMapper.class);

            //设置运行的reducer类
            job.setReducerClass(WordCountReduce.class);

            //设置mapper(key,value)输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reducer(key,value)输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置文件输入输出路径
            FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
            FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

            boolean result = job.waitForCompletion(true);

            System.exit(result ? 0 : -1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
