package com.study.thread;

/**
 * @author 黄思佳
 * @description:
 * @date 2019/9/8 13:56
 */
public class RunnableDemo implements Runnable{
    private String threadName;
    private Thread thread;

    RunnableDemo(String threadName){
        this.threadName = threadName;
        System.out.println("Create Thread:" + threadName);
    }

    @Override
    public void run() {
        System.out.println("Running Thread:" + threadName);
        try {
            for (int i = 0; i < 4; i++) {
                System.out.println("Thread: " + threadName + ", " + i);
                Thread.sleep(50);
            }
        } catch (InterruptedException e){
            System.out.println("Thread " +  threadName + " interrupted.");
        }
        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start(){
        System.out.println("Starting " +  threadName );
        if (thread == null) {
            thread = new Thread (this, threadName);
            thread.start();
        }
    }

    public static void main(String args[]) {
        RunnableDemo R1 = new RunnableDemo( "Thread-1");
        R1.start();

        RunnableDemo R2 = new RunnableDemo( "Thread-2");
        R2.start();
    }
}
