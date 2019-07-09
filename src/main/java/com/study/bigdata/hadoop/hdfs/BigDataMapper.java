package com.study.bigdata.hadoop.hdfs;

public interface BigDataMapper {

    void wcMap(String line, WordCountContext wordCountContext);
}
