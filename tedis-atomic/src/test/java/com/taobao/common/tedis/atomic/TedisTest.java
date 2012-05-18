/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.atomic;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TedisTest {

    static String ip1 = "192.168.0.1";
    static String ip2 = "192.168.0.2";
    static String ip2pass = "password";
    static final int SIZE = 100;

    @Test
    public void main() throws InterruptedException {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(SIZE, SIZE, 1000, TimeUnit.MILLISECONDS, queue);
        long time = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            executor.submit(new Runnable() {
                final Tedis tedis = new Tedis(ip1);
                public void run() {
                    for (int j = 0; j <= 2000; j++) {
                        tedis.set("foo".getBytes(), "bar".getBytes());
                        if (j % 1000 == 0) {
                            System.out.println("finished:" + j);
                        }
                    }
                }
            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("qps:" + (SIZE * 2000) / ((System.currentTimeMillis() - time) / 1000));
    }

}
