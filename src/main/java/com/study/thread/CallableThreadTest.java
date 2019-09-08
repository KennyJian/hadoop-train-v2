package com.study.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @author 黄思佳
 * @description:
 * @date 2019/9/8 15:50
 */
public class CallableThreadTest implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        int i = 0;
        for (; i < 100 ; i++) {
            System.out.println(Thread.currentThread().getName() + " " + i);
        }
        return i;
    }

    public static void main(String[] args) {
        //创建实例
        CallableThreadTest callableThreadTest = new CallableThreadTest();
        //创建FutureTack并包装Callable对象，泛型值为Callable的泛型类型
        //有返回值的线程，异步处理
        FutureTask<Integer> futureTask = new FutureTask<>(callableThreadTest);
        for (int i = 0; i < 100; i++) {
            //主线程
            System.out.println(Thread.currentThread().getName() + " 的循环变量i的值" + i);
            if (i == 20){
                //子线程
                new Thread(futureTask, "childThread").start();
            }
        }
        if (futureTask.isDone()){
            try {
                System.out.println("子线程的返回值：" + futureTask.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
