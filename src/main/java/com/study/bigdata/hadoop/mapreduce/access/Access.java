package com.study.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂类型数据规范
 * 1）根据hadoop规范，实现Writable接口
 * 2）实现write和readFields方法
 * 3）定义一个默认的构造方法
 *
 * @Author: 黄思佳
 * @Date: 2019/7/31 11:19
 */
public class Access implements Writable {
    //手机号
    private String phone;
    //上行流量
    private long up;
    //下行流量
    private long down;
    //上行流量+下行流量
    private long sum;

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    @Override
    public String toString() {
        return phone + ", " + up + "," + down + ", " + sum ;
    }

    public Access() {}

    public Access(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }
}
