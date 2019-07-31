package com.study.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner决定maptask输出的数据交由哪个reducetask处理
 * 默认实现：分发的key的hash值与reduce task个数取模
 *
 * MapReduce自定义分区规则
 * @Author: 黄思佳
 * @Date: 2019/7/31 15:26
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numReduceTasks) {

        if (text.toString().startsWith("13")){
            return 0;
        } else if (text.toString().startsWith("15")){
            return 1;
        }
        return 2;
    }
}
