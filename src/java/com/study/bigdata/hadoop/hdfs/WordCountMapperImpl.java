package com.study.bigdata.hadoop.hdfs;

public class WordCountMapperImpl implements BigDataMapper {

    @Override
    public void wcMap(String line, WordCountContext wordCountContext) {
        String[] words = line.split("\t");
        for (String word : words) {
            Object object = wordCountContext.read(word);
            int count = object == null ? 1 : Integer.parseInt(object.toString()) + 1;
            wordCountContext.write(word, count);
        }
    }
}
