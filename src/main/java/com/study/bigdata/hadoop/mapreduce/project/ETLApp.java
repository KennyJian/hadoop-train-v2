package com.study.bigdata.hadoop.mapreduce.project;

import com.study.bigdata.hadoop.mapreduce.utils.ContentUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLApp {
    public static void main(String[] args) throws Exception{

//        System.setProperty("hadoop.home.dir", "D:/document/hadoop/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ETLApp.class);
        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

        LogParser logParser ;
        @Override
        protected void setup(Context context) {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> map = logParser.parser(value.toString());

            String topicId = ContentUtils.getPageId(map.get("url"));
            String stringBuilder = map.get("time") + "\t" +
                    map.get("country") + "\t" +
                    map.get("province") + "\t" +
                    map.get("city") + "\t" +
                    map.get("url") + "\t" +
                    (StringUtils.isEmpty(topicId) ? "-" : topicId);
            context.write(NullWritable.get(), new Text(stringBuilder));
        }
    }
}
