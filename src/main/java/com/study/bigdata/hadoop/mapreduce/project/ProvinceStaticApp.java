package com.study.bigdata.hadoop.mapreduce.project;

import com.study.bigdata.hadoop.mapreduce.utils.IPParser;
import com.study.bigdata.hadoop.mapreduce.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ProvinceStaticApp {
    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "D:/document/hadoop/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProvinceStaticApp.class);
        job.setMapperClass(ProvinceStaticApp.MyMapper.class);
        job.setReducerClass(ProvinceStaticApp.MyReduce.class);

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

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            LongWritable ONE = new LongWritable(1);

            Map<String, String> map = logParser.parser(value.toString());
            String ip = map.get("ip");
            if (StringUtils.isEmpty(ip)){
                context.write(new Text("-"), ONE);
            } else {
                IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
                if (regionInfo == null || StringUtils.isEmpty(regionInfo.getProvince())){
                    context.write(new Text("-"), ONE);
                } else {
                    context.write(new Text(regionInfo.getProvince()), ONE);
                }
            }
        }
    }

    static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
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
