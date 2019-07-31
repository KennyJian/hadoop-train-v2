package com.study.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 本地运行增加Combiner操作
 * 优点：减少IO，提升作业的执行性能
 * 局限性：求平均数，除法操作要谨用
 *
 * @Author: 黄思佳
 * @Date: 2019/7/22 10:26
 */
public class WordCountCombinerLocalApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration configuration = new Configuration();
        System.setProperty("hadoop.home.dir", "E:/hadoop-2.6.0-cdh5.15.1");

        //创建job
        Job job = Job.getInstance(configuration);

        //设置job运行的主类
        job.setJarByClass(WordCountCombinerLocalApp.class);

        //设置运行的mapper类
        job.setMapperClass(WordCountMapper.class);

        //设置运行的reducer类
        job.setReducerClass(WordCountReduce.class);

        //添加Combiner的设置
        job.setCombinerClass(WordCountReduce.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer(key,value)输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}
