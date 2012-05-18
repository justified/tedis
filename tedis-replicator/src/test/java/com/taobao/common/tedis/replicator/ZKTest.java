/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator;

import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

import com.taobao.common.tedis.dislock.CallBack;
import com.taobao.common.tedis.dislock.Lock;
import com.taobao.common.tedis.dislock.ZKClient;
import com.taobao.common.tedis.dislock.ZKException;
import com.taobao.common.tedis.dislock.ZKLockFactory;
import com.taobao.common.tedis.util.ZKUtil;

public class ZKTest {

    @Test
    public void testLock() throws Exception {
        BasicConfigurator.configure();
        final ZKLockFactory zkLockFactory = new ZKLockFactory();
        Properties properties = new Properties();
        properties.put("zkAddress", "localhost:2181");
        properties.put("zkTimeout", "500000");
        properties.put("appName", "tedis");
        zkLockFactory.setProperties(properties);
        zkLockFactory.init();
        final Lock lock = zkLockFactory.getLock("replicator-lock");
        lock.registerListener(new CallBack() {
            @Override
            public void doTask(String path, Object ctx) {
                System.out.println("Path:" + path);
                System.out.println("Ctx:" + ctx);
                if (lock.tryLock()) {
                    System.out.println("Try lock successful!");
                } else {
                    System.out.println("Try lock failed.");
                }
            }
        }, "Hello world.");

        if (lock.tryLock()) {
            System.out.println("Try lock successful!");
        } else {
            System.out.println("Try lock failed.");
        }
        Thread.sleep(10000);
    }

    @Test
    public void testConfig() throws Exception {
        final ZKClient client = new ZKClient("localhost:2181", 500000);
        client.init();


        String path = ZKUtil.contact("tedis", "replicator-config");
        path = ZKUtil.contact(ZKUtil.MUTEXLOCK_ROOT, path);
        path = ZKUtil.normalize(path);



        if (client.useAble()) {
            client.delete(path);

            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    String path = ZKUtil.contact("tedis", "replicator-config");
                    path = ZKUtil.contact(ZKUtil.MUTEXLOCK_ROOT, path);
                    path = ZKUtil.normalize(path);
                    try {
                        client.rcreate(path, "word".getBytes());
                    } catch (ZKException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).start();

            if (!client.exists(path)) {
                client.rcreate(path, "test".getBytes());
                System.out.println("create path");
            }
            System.out.println(path);
            System.out.println(new String(client.getData(path, new Watcher() {

                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.getPath());
                    try {
                        System.out.println(new String(client.getData(event.getPath())));
                    } catch (ZKException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            })));

            Thread.sleep(10000);
        }


    }
}
