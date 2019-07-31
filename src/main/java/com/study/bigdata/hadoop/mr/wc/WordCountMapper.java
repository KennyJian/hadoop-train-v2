package com.study.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN：Map任务读取数据的key类型，offset，是每行数据起始位置的偏移量，Long
 * VALUEIN：Map任务读取数据的Value类型，其实就是一行行的字符串，String
 * KEYOUT：Map方法自定义实现输出的key的类型，String
 * VALUEOUT：Map方法自定义实现输出的valud的类型，Integer
 *
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
