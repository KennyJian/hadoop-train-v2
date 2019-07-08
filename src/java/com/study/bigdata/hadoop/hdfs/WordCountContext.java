package com.study.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 词频统计上下文
 */
public class WordCountContext {
    private Map<Object, Object> map = new HashMap<>();

    public void write(Object key , Object value){
        map.put(key, value);
    }

    public Object read(Object key){
        return map.get(key);
    }

    public Map<Object, Object> getWordCountContext(){
        return map;
    }
}
