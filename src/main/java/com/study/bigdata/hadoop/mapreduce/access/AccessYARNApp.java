package com.study.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 黄思佳
 * @Date: 2019/7/31 14:04
 */
public class AccessYARNApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "E:/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessYARNApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReduce.class);
        /**
         * big坑：
         * 如果Reduce输出Key的类型为NullWritable，设置了CombinerClass会报输出类型错误
         * 可能性：因为Combiner是一个中间过程，如果key设置为null后面就无法通过键值获取了
         */
//        job.setCombinerClass(AccessReduce.class);

        //设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        //设置分区个数：这个数量要个自定义的Partitioner类中定义的个数保持一致
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
