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

            IPParser ipParser = IPParser.getInstance();
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
            if (regionInfo != null){
                map.put("country", regionInfo.getCountry());
                map.put("province", regionInfo.getProvince());
                map.put("city", regionInfo.getCity());
            }
        }
        return map;
    }
}
