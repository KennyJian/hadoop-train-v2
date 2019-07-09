package com.study.bigdata.hadoop.hdfs;

import com.study.bean.ParamUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.study.bean.Constants.*;

/**
 * 词频统计
 * 1、打开文件
 * 2、词频统计
 * 3、数据缓存
 * 4、输出结果
 */
public class HDFSWCApp01 {

    public static void main(String[] args) throws Exception {

        Properties properties = ParamUtils.getProperties();

        //连接hdfs，获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(HDFS_PATH)), new Configuration(), properties.getProperty(USER_NAME));
        //上下文
        WordCountContext wordCountContext = new WordCountContext();
        //缓存对象
//        BigDataMapper bigDataMapper = new WordCountMapperImpl();
        //配置实现类，通过反射获取
        Class<?> clazz = Class.forName((String) properties.get(MAPPER_CALSS));
        BigDataMapper bigDataMapper = (BigDataMapper) clazz.newInstance();


        //1、打开文件
        LocatedFileStatus locatedFileStatus;
        FSDataInputStream fsDataInputStream;
        BufferedReader bufferedReader;
        String line;
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(properties.getProperty(INPUT_PATH)), false);
        while (iterator.hasNext()){
            locatedFileStatus = iterator.next();
            fsDataInputStream = fileSystem.open(locatedFileStatus.getPath());
            bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

            while ((line = bufferedReader.readLine()) != null){
                //2、处理每行数据，进行词频统计
                bigDataMapper.wcMap(line, wordCountContext);
            }
            fsDataInputStream.close();
            bufferedReader.close();
        }

        //3、数据缓存
        Map<Object, Object> resultMap = wordCountContext.getWordCountContext();

        //4、输出缓存结果
        Path path = new Path(properties.getProperty(OUTPUT_PATH));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        for (Map.Entry<Object, Object> entry : resultMap.entrySet()) {
            fsDataOutputStream.writeUTF(entry.getKey() + " : " + entry.getValue() + "\n");
        }

        fsDataOutputStream.close();
        System.out.println("-----------------统计结束----------------");
    }
}
