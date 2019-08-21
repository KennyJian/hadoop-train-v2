package com.study.bigdata.hadoop.mapreduce.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    public Map<String, String> parser(String line){
        Map<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(line)){

            String[] items = line.split("\001");
            String ip = items[13];
            map.put("ip", ip);
            String url = items[1];
            map.put("url", url);
            String time = items[17];
            map.put("time", time);

            IPParser ipParser = IPParser.getInstance();
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
            if (regionInfo != null){
                map.put("country", regionInfo.getCountry());
                map.put("province", regionInfo.getProvince());
                map.put("city", regionInfo.getCity());
            } else {
                map.put("country", "-");
                map.put("province", "-");
                map.put("city", "-");
            }
        }
        return map;
    }

    public Map<String, String> parserV2(String line){
        Map<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(line)){
            String[] items = line.split("\t");
            map.put("time", items[0]);
            map.put("province", items[2]);
            map.put("url", items[3]);
            map.put("pageId", items[4]);
        }
        return map;
    }
}
