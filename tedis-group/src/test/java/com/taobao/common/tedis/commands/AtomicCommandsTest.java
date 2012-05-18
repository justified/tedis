/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.AtomicCommands;
import com.taobao.common.tedis.group.ReliableAsynTedisGroup;

public class AtomicCommandsTest extends BaseTestCase {
    private static AtomicCommands atomicCommands;
    long value1 = 1l;
    long value2 = 2l;
    long value3 = 3l;

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new ReliableAsynTedisGroup(appName, version);
        tedisGroup.init();
        atomicCommands = new DefaultAtomicCommands(tedisGroup.getTedis());
        tedisGroup.getTedis().flushAll();
    }

    @Test
    public void setGetTest() {
        atomicCommands.set(NAMESPACE1, key1, value1);
        assertNotNull(atomicCommands.get(NAMESPACE1, key1));
        assertEquals(value1, atomicCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void msetTest() {
        Map<Key, Long> map = new HashMap<Key, Long>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        atomicCommands.multiSet(NAMESPACE2, map);
        assertEquals(value1, atomicCommands.get(NAMESPACE2, key1));
        assertEquals(value2, atomicCommands.get(NAMESPACE2, key2));
        assertEquals(value3, atomicCommands.get(NAMESPACE2, key3));
    }

    @Test
    public void mgetTest() {
        Map<Key, Long> map = new HashMap<Key, Long>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        atomicCommands.multiSet(NAMESPACE2, map);
        Collection<Key> keys = new ArrayList<Key>();
        keys.add(key2);
        keys.add(key3);
        keys.add(key1);
        keys.add(new Key(404));
        List<Long> list = atomicCommands.multiGet(NAMESPACE2, keys);
        assertEquals(4, list.size());
        assertEquals(value1, list.get(2).longValue());
        assertTrue(list.get(3) == 0);
    }

    @Test
    public void multiSetIfAbsent() {
        atomicCommands.set(NAMESPACE1, key1, value2);
        Map<Key, Long> map = new HashMap<Key, Long>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        atomicCommands.multiSetIfAbsent(NAMESPACE1, map);
        assertNotSame(value1, atomicCommands.get(NAMESPACE1, key1));
        assertEquals(value2, atomicCommands.get(NAMESPACE1, key1));
        assertTrue(atomicCommands.get(NAMESPACE1, key2) == 0);
    }

    @Test
    public void getAndSet() {
        atomicCommands.set(NAMESPACE1, key1, value1);
        long old = atomicCommands.getAndSet(NAMESPACE1, key1, value2);
        assertEquals(value1, old);
        assertEquals(value2, atomicCommands.get(NAMESPACE1, key1));
        old = atomicCommands.getAndSet(NAMESPACE1, new Key((int)System.currentTimeMillis()), value1);
        System.out.println(old);
        assertEquals(0, old);
    }

    @Test
    public void increment() {
        atomicCommands.set(NAMESPACE1, key1, value1);
        assertTrue(atomicCommands.increment(NAMESPACE1, key1, 1) == 2);
        assertTrue(atomicCommands.get(NAMESPACE1, key1) == 2);
        assertTrue(atomicCommands.increment(NAMESPACE1, key1, 5) == 7);
    }

}
