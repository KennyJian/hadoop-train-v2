package com.study.bigdata.hadoop.mapreduce.project;

import com.study.bigdata.hadoop.mapreduce.utils.ContentUtils;
import com.study.bigdata.hadoop.mapreduce.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStaticApp {
    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "D:/document/hadoop/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PageStaticApp.class);
        job.setMapperClass(PageStaticApp.MyMapper.class);
        job.setReducerClass(PageStaticApp.MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("output/raw");
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, new Path("input/raw"));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            LongWritable ONE = new LongWritable(1);

            Map<String, String> map = logParser.parser(value.toString());
            String url = map.get("url");
            String pageId = ContentUtils.getPageId(url);
            context.write(new Text(pageId), ONE);
        }
    }

    static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}
