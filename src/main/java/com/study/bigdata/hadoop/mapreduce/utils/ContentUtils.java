package com.study.bigdata.hadoop.mapreduce.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentUtils {

    public static String getPageId(String url){

        if (StringUtils.isEmpty(url)){
            return "";
        }

        Pattern pattern = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pattern.matcher(url);
        if (!matcher.find()){
            return "";
        }
        return matcher.group().split("topicId=")[1];
    }
}
