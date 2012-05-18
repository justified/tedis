/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands.benchmark;

import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.taobao.common.tedis.commands.BaseTestCase;
import com.taobao.common.tedis.commands.TedisManagerFactory;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.group.TedisGroup;
import com.taobao.common.tedis.serializer.StringTedisSerializer;
import com.taobao.common.tedis.util.SortParams;

public class CopyOfBenchmarkTest extends BaseTestCase {

    public static TedisGroup tedisGroup;

    public static TedisManager tedisManager = TedisManagerFactory.create("test", "v0", new StringTedisSerializer());

    public static int namespace = 10;

    public static final int SIZE = 10000;

    public static final String ITEM_PRICE = "item-price";

    public static final String ITEM_SOLD = "item-sold";

    public static final String CAT_ALL = "catall";

    public static final String CAT_1 = "cat1";

    @BeforeClass
    public static void setup() {
        try {
            tedisGroup = new TedisGroup(appName, version);
            tedisGroup.init();
            tedisGroup.getTedis().flushAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSort() throws Exception {
        initData();
        final SortParams params = new SortParams().by(namespace, ITEM_PRICE + "*").limit(0, 19).desc();
        RequestBlock requestBlock = new RequestBlock() {

            @Override
            public void execute() {
                tedisManager.sort(namespace, CAT_ALL, params);
            }
        };
        testPerformce("testSort", requestBlock, default_time, default_thread_count);
        cleanData();
    }

    @Test
    public void testInter() throws Exception {
        initData();
        RequestBlock requestBlock = new RequestBlock() {

            @Override
            public void execute() {
                tedisManager.getSetCommands().intersect(namespace, CAT_ALL, CAT_1);
            }
        };
        testPerformce("testInter", requestBlock, default_time, default_thread_count);
        cleanData();
    }


    private static void initData() {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < SIZE; i++) {
            tedisManager.getAtomicCommands().set(namespace, ITEM_PRICE + (10000 + i), 100 + random.nextInt(10000));
            tedisManager.getAtomicCommands().set(namespace, ITEM_SOLD + (10000 + i), random.nextInt(1000));
            tedisManager.getSetCommands().add(namespace, CAT_ALL, String.valueOf(10000 + i));
            tedisManager.getSetCommands().add(namespace, CAT_1, String.valueOf(10000 + i));
        }
    }

    private static void cleanData() {
        tedisGroup.getTedis().flushAll();
    }

    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(CopyOfBenchmarkTest.class);
        System.out.println("runtime:" + result.getRunTime());
        System.out.println("runcount:" + result.getRunCount());
        System.out.println("failurecout:" + result.getFailureCount());
    }

}
