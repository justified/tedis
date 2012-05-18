/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;

public class BaseTestCase extends Assert {

    public static final String appName = "test";

    public static final String version = "v0";


    public static long default_time = 1000 * 30;// 30s
    public static int default_thread_count = 20;

    static volatile long max;
    static volatile long min;

    @SuppressWarnings("unchecked")
    public void testPerformce(String message, RequestBlock requestBlock, long test_time, int thread_count) throws Exception {
        max = 0;
        min = 1000;
        stop = false;
        System.out.println("--------------test start:" + message + "----------------");
        int t = thread_count;
        ExecutorService exec = Executors.newFixedThreadPool(t);
        CountDownLatch cdh = new CountDownLatch(t);
        Future<Integer>[] calls = new Future[t];
        for (int i = 0; i < t; i++) {
            calls[i] = exec.submit(new RequestCallable(cdh, requestBlock));
        }
        long time = System.currentTimeMillis();
        Thread.sleep(test_time);
        stop = true;
        long total = 0;
        for (Future<Integer> call : calls) {
            total += call.get().longValue();
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time:" + time);
        System.out.println("total:" + total);
        long qps = total / (time / 1000);
        System.out.println("qps:" + qps);
        System.out.println("min:" + (min + 1) / (1000.0 * 1000.0) + ",max:" + max / (1000.0 * 1000.0) + ",avg:" + (1000.0 / qps) * 5);
        System.out.println("--------------test end:" + message + "----------------");
        exec.shutdownNow();
        Thread.sleep(1000 * 3);
    }

    public static volatile boolean stop = false;

    public static class RequestCallable implements Callable<Integer> {

        int count;
        CountDownLatch cdh;
        int errorCount;
        RequestBlock requestBlock;

        RequestCallable(CountDownLatch cdh, RequestBlock requestBlock) {
            this.cdh = cdh;
            this.requestBlock = requestBlock;
        }

        @Override
        public Integer call() throws Exception {
            cdh.countDown();
            cdh.await();
            while (!stop) {
                try {
                    long t = System.nanoTime();
                    requestBlock.execute();
                    t = System.nanoTime() - t;
                    if (max < t) {
                        max = t;
                    }
                    if (min > t) {
                        min = t;
                    }
                    count++;
                } catch (Throwable e) {
                    errorCount++;
                    e.printStackTrace();
                    Thread.sleep(10000);
                    System.exit(0);
                }
            }
            return count;
        }
    }

    public abstract class RequestBlock {

        public abstract void execute();
    }

    public static class Key implements Serializable {
        private static final long serialVersionUID = 4357644052290181308L;

        public Integer key = 0;

        public Key(int key) {
            super();
            this.key = key;
        }

        public int getKey() {
            return key;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Key)) {
                return false;
            }
            return ((Key)obj).key.equals(this.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

    }
    public static class Message implements Serializable{
        private static final long serialVersionUID = 1L;
        private String message;

        public Message(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Message)) {
                return false;
            }
            return ((Message)obj).getMessage() != null && ((Message)obj).getMessage().equals(this.message);
        }

        @Override
        public int hashCode() {
            return message.hashCode();
        }

        @Override
        public String toString() {
            return "Message [message=" + message + "]";
        }

    }

    protected static final int NAMESPACE1 = 11;
    protected static final int NAMESPACE2 = 12;
    protected static final int NAMESPACE3 = 13;
    protected static final Key key1 = new Key(1);
    protected static final Key key2 = new Key(2);
    protected static final Key key3 = new Key(3);
    protected static final Message message1 = new Message("this is message1");
    protected static final Message message2 = new Message("this is message2");
    protected static final Message message3 = new Message("this is message3");

}
