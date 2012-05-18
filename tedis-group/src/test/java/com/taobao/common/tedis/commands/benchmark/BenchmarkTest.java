/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands.benchmark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.commands.BaseTestCase;
import com.taobao.common.tedis.commands.DefaultTedisManager;
import com.taobao.common.tedis.commands.TedisManagerFactory;
import com.taobao.common.tedis.core.HashCommands;
import com.taobao.common.tedis.core.ListCommands;
import com.taobao.common.tedis.core.SetCommands;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.core.ValueCommands;
import com.taobao.common.tedis.core.ZSetCommands;
import com.taobao.common.tedis.group.TedisGroup;
import com.taobao.common.tedis.serializer.StringTedisSerializer;
import com.taobao.common.tedis.util.SortParams;

public class BenchmarkTest extends BaseTestCase {

    static Group tedisGroup;

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
    public void testGetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ValueCommands valueCommands = tedisManager.getValueCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        valueCommands.set(0, key, item);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                valueCommands.get(0, key);
            }
        };
        testPerformce("testGetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testSetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ValueCommands valueCommands = tedisManager.getValueCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                valueCommands.set(0, key, item);
            }
        };
        testPerformce("testSetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testMultiGetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ValueCommands valueCommands = tedisManager.getValueCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key0 = random.nextLong();
        final Long key1 = random.nextLong();
        final Long key2 = random.nextLong();
        final Long key3 = random.nextLong();
        final Long key4 = random.nextLong();
        final Long key5 = random.nextLong();
        final Long key6 = random.nextLong();
        final Long key7 = random.nextLong();
        final Long key8 = random.nextLong();
        final Long key9 = random.nextLong();
        valueCommands.set(0, key0, item);
        valueCommands.set(0, key1, item);
        valueCommands.set(0, key2, item);
        valueCommands.set(0, key3, item);
        valueCommands.set(0, key4, item);
        valueCommands.set(0, key5, item);
        valueCommands.set(0, key6, item);
        valueCommands.set(0, key7, item);
        valueCommands.set(0, key8, item);
        valueCommands.set(0, key9, item);
        final Long[] array = new Long[]{key0, key1, key2, key3, key4, key5, key6, key7, key8, key9};
        final Collection<Long> keys = Arrays.asList(array);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                List<ItemData> list = valueCommands.multiGet(0, keys);
                assertEquals(10, list.size());
            }
        };
        testPerformce("testMultiGetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, keys);
    }

    @Test
    public void testMultiSetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ValueCommands valueCommands = tedisManager.getValueCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key0 = random.nextLong();
        final Long key1 = random.nextLong();
        final Long key2 = random.nextLong();
        final Long key3 = random.nextLong();
        final Long key4 = random.nextLong();
        final Long key5 = random.nextLong();
        final Long key6 = random.nextLong();
        final Long key7 = random.nextLong();
        final Long key8 = random.nextLong();
        final Long key9 = random.nextLong();
        final Long[] array = new Long[]{key0, key1, key2, key3, key4, key5, key6, key7, key8, key9};
        final Collection<Long> keys = Arrays.asList(array);
        final Map<Long, ItemData> values = new HashMap<Long, ItemData>();
        values.put(key0, item);
        values.put(key1, item);
        values.put(key2, item);
        values.put(key3, item);
        values.put(key4, item);
        values.put(key5, item);
        values.put(key6, item);
        values.put(key7, item);
        values.put(key8, item);
        values.put(key9, item);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                valueCommands.multiSet(0, values);
            }
        };
        testPerformce("testMultiSetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, keys);
    }

    @Test
    public void testLeftPushItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ListCommands listCommands = tedisManager.getListCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                listCommands.leftPush(1, key, item);
            }
        };
        testPerformce("testLeftPushItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testRangeItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ListCommands listCommands = tedisManager.getListCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        listCommands.leftPush(1, key, item);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                List<ItemData> list = listCommands.range(1, key, 0, 9);
                assertEquals(10, list.size());
            }
        };
        testPerformce("testRangeItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testHashSetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final HashCommands hashCommands = tedisManager.getHashCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                hashCommands.put(3, key, key, item);
            }
        };
        testPerformce("testHashSetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testHashGetItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final HashCommands hashCommands = tedisManager.getHashCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        hashCommands.put(3, key, key, item);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                assertNotNull(hashCommands.get(3, key, key));
            }
        };
        testPerformce("testHashGetItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testSetAddItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final SetCommands setCommands = tedisManager.getSetCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                setCommands.add(4, key, item);
            }
        };
        testPerformce("testSetAddItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testSetMembersItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final SetCommands setCommands = tedisManager.getSetCommands();
        final ItemData item0 = new ItemData(0);
        final ItemData item1 = new ItemData(1);
        final ItemData item2 = new ItemData(2);
        final ItemData item3 = new ItemData(3);
        final ItemData item4 = new ItemData(4);
        final ItemData item5 = new ItemData(5);
        final ItemData item6 = new ItemData(6);
        final ItemData item7 = new ItemData(7);
        final ItemData item8 = new ItemData(8);
        final ItemData item9 = new ItemData(9);
        Random random = new Random();
        final Long key = random.nextLong();
        setCommands.add(4, key, item0);
        setCommands.add(4, key, item1);
        setCommands.add(4, key, item2);
        setCommands.add(4, key, item3);
        setCommands.add(4, key, item4);
        setCommands.add(4, key, item5);
        setCommands.add(4, key, item6);
        setCommands.add(4, key, item7);
        setCommands.add(4, key, item8);
        setCommands.add(4, key, item9);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                Set<ItemData> sets = setCommands.members(4, key);
                assertEquals(10, sets.size());
            }
        };
        testPerformce("testSetMembersItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testZSetAddItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ZSetCommands zSetCommands = tedisManager.getZSetCommands();
        final ItemData item = new ItemData();
        Random random = new Random();
        final Long key = random.nextLong();
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                zSetCommands.add(4, key, item, 0);
            }
        };
        testPerformce("testZSetAddItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
    }

    @Test
    public void testZSetRangeItem() throws NoSuchMethodException, Exception {
        final TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        final ZSetCommands zSetCommands = tedisManager.getZSetCommands();
        final ItemData item0 = new ItemData(0);
        final ItemData item1 = new ItemData(1);
        final ItemData item2 = new ItemData(2);
        final ItemData item3 = new ItemData(3);
        final ItemData item4 = new ItemData(4);
        final ItemData item5 = new ItemData(5);
        final ItemData item6 = new ItemData(6);
        final ItemData item7 = new ItemData(7);
        final ItemData item8 = new ItemData(8);
        final ItemData item9 = new ItemData(9);
        Random random = new Random();
        final Long key = random.nextLong();
        zSetCommands.add(4, key, item0, 0);
        zSetCommands.add(4, key, item1, 1);
        zSetCommands.add(4, key, item2, 2);
        zSetCommands.add(4, key, item3, 3);
        zSetCommands.add(4, key, item4, 4);
        zSetCommands.add(4, key, item5, 5);
        zSetCommands.add(4, key, item6, 6);
        zSetCommands.add(4, key, item7, 7);
        zSetCommands.add(4, key, item8, 8);
        zSetCommands.add(4, key, item9, 9);
        RequestBlock reqMethod = new RequestBlock() {
            @Override
            public void execute() {
                Set<ItemData> sets = zSetCommands.range(4, key, 0, 9);
                assertEquals(10, sets.size());
            }
        };
        testPerformce("testZSetRangeItem", reqMethod, default_time, default_thread_count);
        tedisManager.delete(0, key);
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
    }

    public static TedisManager tedisManager = TedisManagerFactory.create("test", "v0", new StringTedisSerializer());

    public static int namespace = 10;

    public static final int SIZE = 1000;

    public static final String ITEM_PRICE = "item-price";

    public static final String ITEM_SOLD = "item-sold";

    public static final String CAT_ALL = "catall";


    private static void initData() {
        Random random = new Random(System.currentTimeMillis());
        for(int i = 0; i < SIZE; i++) {
            tedisManager.getAtomicCommands().set(namespace, ITEM_PRICE + (10000 + i), 100 + random.nextInt(10000));
            tedisManager.getAtomicCommands().set(namespace, ITEM_SOLD + (10000 + i), random.nextInt(1000));
            tedisManager.getSetCommands().add(namespace, CAT_ALL, String.valueOf(10000 + i));
        }
    }

    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(BenchmarkTest.class);
        System.out.println("runtime:" + result.getRunTime());
        System.out.println("runcount:" + result.getRunCount());
        System.out.println("failurecout:" + result.getFailureCount());
    }


}
