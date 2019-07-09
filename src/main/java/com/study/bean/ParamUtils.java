package com.study.bean;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: 黄思佳
 * @Date: 2019/7/9 9:38
 */
public class ParamUtils {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

}
