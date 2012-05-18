/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.taobao.common.tedis.group;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.taobao.common.tedis.binary.RedisCommands;
import com.taobao.common.tedis.group.ReliableAsynTedisGroup;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @version 1.0
 */
public class ReliableAsynTedisGroupTest {

    public static long default_time = 1000 * 30;// 30s
    public static int default_thread_count = 10;

    static volatile long max;
    static volatile long min;

    public static void main(String[] args) {
        ReliableAsynTedisGroup tedisGroup = new ReliableAsynTedisGroup("test", "v0");
        tedisGroup.getTedis().set("test".getBytes(), "echo".getBytes());
    }

    @SuppressWarnings("unchecked")
    public void testPerformce(String message, RequestMethod reqMethod, long test_time, int thread_count) throws Exception {
        max = min = 0;
        stop = false;
        System.out.println("--------------test start:" + message + "----------------");
        int t = thread_count;
        ExecutorService exec = Executors.newFixedThreadPool(t);
        CountDownLatch cdh = new CountDownLatch(t);
        Future<Integer>[] calls = new Future[t];
        for (int i = 0; i < t; i++) {
            calls[i] = exec.submit(new RequestCallable(cdh, reqMethod));
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
        RequestMethod requestMethod;

        RequestCallable(CountDownLatch cdh, RequestMethod requestMethod) {
            this.cdh = cdh;
            this.requestMethod = requestMethod;
        }

        @Override
        public Integer call() throws Exception {
            cdh.countDown();
            cdh.await();
            System.out.println("开始运行:" + Thread.currentThread().getName());
            while (!stop) {
                try {
                    long t = System.nanoTime();
                    requestMethod.m.invoke(requestMethod.tedis, requestMethod.args);
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
                    Thread.sleep(100);
                }
            }
            System.out.println("stop:" + Thread.currentThread().getName() + "errorCount=" + errorCount);
            return count;
        }
    }

    public class RequestMethod {
        Method m;
        Object[] args;
        RedisCommands tedis;

        public RequestMethod(Method m, Object[] args, RedisCommands tedis) {
            this.m = m;
            this.args = args;
            this.tedis = tedis;
        }
    }

    @SuppressWarnings("unchecked")
    public static Class[] genClass(Object[] args) {
        Class[] ret = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            ret[i] = args[i].getClass();
        }
        return ret;
    }

    @Test
    public void testSet128b() throws NoSuchMethodException, Exception {
        RedisCommands tedis = new ReliableAsynTedisGroup("test", "v0").getTedis();
        byte[] key = "testSet128b".getBytes();
        tedis.del(key);// 准备或清理数据
        Object[] args = new Object[] { key, new byte[128] };// 构造要测试的方法参数
        // 反射获取对应的方法
        RequestMethod reqMethod = new RequestMethod(RedisCommands.class.getMethod("set", genClass(args)), args, tedis);
        // 调用测试
        testPerformce("testSet128b", reqMethod, default_time, default_thread_count);
        tedis.del(key);// 清理数据
    }

    @Test
    public void testGet128b() throws NoSuchMethodException, Exception {
        RedisCommands tedis = new ReliableAsynTedisGroup("test", "v0").getTedis();
        byte[] key = "testGet128b".getBytes();
        tedis.set(key, new byte[128]);// //准备或清理数据
        Object[] args = new Object[] { key };
        RequestMethod reqMethod = new RequestMethod(RedisCommands.class.getMethod("get", genClass(args)), args, tedis);
        testPerformce("testGet128b", reqMethod, default_time, default_thread_count);
        tedis.del(key);// 清理数据
    }

}
