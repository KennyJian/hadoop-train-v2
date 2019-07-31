package com.study.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Driver:配置mapper,reduce
 * 连接hadoop运行
 * @Author: 黄思佳
 * @Date: 2019/7/22 10:26
 */
public class WordCountApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        String uri = "hdfs://192.168.147.3:8020";

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "E:/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", uri);

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

        //若输出目录存在文件，则递归删除
        FileSystem fileSystem = FileSystem.get(new URI(uri), configuration, "hadoop");
        Path ouputPath = new Path("/wordcount/output");
        if (fileSystem.exists(ouputPath)){
            fileSystem.delete(ouputPath, true);
        }

        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, ouputPath);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}
