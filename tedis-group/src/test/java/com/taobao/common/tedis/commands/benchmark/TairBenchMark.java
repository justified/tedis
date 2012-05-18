///**
// * (C) 2011-2012 Alibaba Group Holding Limited.
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * version 2 as published by the Free Software Foundation.
// *
// */
//package com.taobao.common.tedis.commands.benchmark;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.Callable;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.junit.runner.JUnitCore;
//import org.junit.runner.Result;
//
//import com.taobao.tair.impl.DefaultTairManager;
//
///**
// * @author juxin.zj E-mail:juxin.zj@taobao.com
// * @since 2011-8-24 下午04:58:31
// * @version 1.0
// */
//public class TairBenchMark {
//    public static long default_time = 1000 * 30;// 30s
//    public static int default_thread_count = 10;
//
//    static volatile long max;
//    static volatile long min;
//
//    private static DefaultTairManager tairManager;
//
//    @BeforeClass
//    public static void init() {
//        try {
//            tairManager = new DefaultTairManager();
//            List<String> configServers = new ArrayList<String>();
//            configServers.add("localhost:5198");
//            tairManager.setConfigServerList(configServers);
//            tairManager.setGroupName("group_1");
//            tairManager.init();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    public static void testPerformce(String message, RequestBlock requestBlock, long test_time, int thread_count) throws Exception {
//        max = 0;
//        min = 1000;
//        stop = false;
//        System.out.println("--------------test start:" + message + "----------------");
//        int t = thread_count;
//        ExecutorService exec = Executors.newFixedThreadPool(t);
//        CountDownLatch cdh = new CountDownLatch(t);
//        Future<Integer>[] calls = new Future[t];
//        for (int i = 0; i < t; i++) {
//            calls[i] = exec.submit(new RequestCallable(cdh, requestBlock));
//        }
//        long time = System.currentTimeMillis();
//        Thread.sleep(test_time);
//        stop = true;
//        long total = 0;
//        for (Future<Integer> call : calls) {
//            total += call.get().longValue();
//        }
//        time = System.currentTimeMillis() - time;
//        System.out.println("time:" + time);
//        System.out.println("total:" + total);
//        long qps = total / (time / 1000);
//        System.out.println("qps:" + qps);
//        System.out.println("min:" + (min + 1) / (1000.0 * 1000.0) + ",max:" + max / (1000.0 * 1000.0) + ",avg:" + (1000.0 / qps) * 5);
//        System.out.println("--------------test end:" + message + "----------------");
//        exec.shutdownNow();
//        Thread.sleep(1000 * 3);
//    }
//
//    public static volatile boolean stop = false;
//
//    public static class RequestCallable implements Callable<Integer> {
//
//        int count;
//        CountDownLatch cdh;
//        int errorCount;
//        RequestBlock requestBlock;
//
//        RequestCallable(CountDownLatch cdh, RequestBlock requestBlock) {
//            this.cdh = cdh;
//            this.requestBlock = requestBlock;
//        }
//
//        @Override
//        public Integer call() throws Exception {
//            cdh.countDown();
//            cdh.await();
//            while (!stop) {
//                try {
//                    long t = System.nanoTime();
//                    requestBlock.execute();
//                    t = System.nanoTime() - t;
//                    if (max < t) {
//                        max = t;
//                    }
//                    if (min > t) {
//                        min = t;
//                    }
//                    count++;
//                } catch (Throwable e) {
//                    errorCount++;
//                    e.printStackTrace();
//                    Thread.sleep(10000);
//                    System.exit(0);
//                }
//            }
//            return count;
//        }
//    }
//
//    public static abstract class RequestBlock {
//
//        public abstract void execute();
//    }
//
//    @Test
//    public void testGetItem() throws NoSuchMethodException, Exception {
//        final ItemData item = new ItemData();
//        Random random = new Random();
//        final Long key = random.nextLong();
//        tairManager.put(0, key, item);
//        RequestBlock reqMethod = new RequestBlock() {
//            @Override
//            public void execute() {
//                tairManager.get(0, key);
//            }
//        };
//        testPerformce("testGetItem", reqMethod, default_time, default_thread_count);
//        tairManager.delete(0, key);
//    }
//
//    @Test
//    public void testSetItem() throws NoSuchMethodException, Exception {
//        final ItemData item = new ItemData();
//        Random random = new Random();
//        final Long key = random.nextLong();
//        RequestBlock reqMethod = new RequestBlock() {
//            @Override
//            public void execute() {
//                tairManager.put(0, key, item);
//            }
//        };
//        testPerformce("testSetItem", reqMethod, default_time, default_thread_count);
//        tairManager.delete(0, key);
//    }
//
//    @Test
//    public void testMultiGetItem() throws NoSuchMethodException, Exception {
//        final ItemData item = new ItemData();
//        Random random = new Random();
//        final Long key0 = random.nextLong();
//        final Long key1 = random.nextLong();
//        final Long key2 = random.nextLong();
//        final Long key3 = random.nextLong();
//        final Long key4 = random.nextLong();
//        final Long key5 = random.nextLong();
//        final Long key6 = random.nextLong();
//        final Long key7 = random.nextLong();
//        final Long key8 = random.nextLong();
//        final Long key9 = random.nextLong();
//        tairManager.put(0, key0, item);
//        tairManager.put(0, key1, item);
//        tairManager.put(0, key2, item);
//        tairManager.put(0, key3, item);
//        tairManager.put(0, key4, item);
//        tairManager.put(0, key5, item);
//        tairManager.put(0, key6, item);
//        tairManager.put(0, key7, item);
//        tairManager.put(0, key8, item);
//        tairManager.put(0, key9, item);
//        final Long[] array = new Long[] { key0, key1, key2, key3, key4, key5, key6, key7, key8, key9 };
//        final List<Long> keys = Arrays.asList(array);
//        RequestBlock reqMethod = new RequestBlock() {
//            @Override
//            public void execute() {
//                tairManager.mget(0, keys).getValue();
//            }
//        };
//        testPerformce("testMultiGetItem", reqMethod, default_time, default_thread_count);
//        tairManager.mdelete(0, keys);
//    }
//
//    public static void main(String[] args) {
//        Result result = JUnitCore.runClasses(TairBenchMark.class);
//        System.out.println("runtime:" + result.getRunTime());
//        System.out.println("runcount:" + result.getRunCount());
//        System.out.println("failurecout:" + result.getFailureCount());
//    }
//}
