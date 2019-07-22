package com.study.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 黄思佳
 * @Date: 2019/7/22 9:45
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
