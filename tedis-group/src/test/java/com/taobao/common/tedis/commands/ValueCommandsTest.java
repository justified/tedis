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
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.ValueCommands;
import com.taobao.common.tedis.group.TedisGroup;

public class ValueCommandsTest extends BaseTestCase {
    private static ValueCommands valueCommands;

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        valueCommands = new DefaultValueCommands(tedisGroup.getTedis());
    }

    @Test
    public void setGetTest() {
        Message message = valueCommands.get(NAMESPACE1, key3);
        System.out.println(message);
        valueCommands.set(NAMESPACE1, key3, message1);
        assertNotNull(valueCommands.get(NAMESPACE1, key3));
        assertEquals(message1, valueCommands.get(NAMESPACE1, key3));
    }

    @Test
    public void getAndsetTest() {
    	System.out.println(valueCommands.getAndSet(NAMESPACE1, key3, message2));
    }

    @Test
    public void msetTest() {
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        valueCommands.multiSet(NAMESPACE2, map);
        assertEquals(message1, valueCommands.get(NAMESPACE2, key1));
        assertEquals(message2, valueCommands.get(NAMESPACE2, key2));
        assertEquals(message3, valueCommands.get(NAMESPACE2, key3));
    }

    @Test
    public void mgetTest() {
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        valueCommands.multiSet(NAMESPACE2, map);
        Collection<Key> keys = new ArrayList<Key>();
        keys.add(key2);
        keys.add(key3);
        keys.add(key1);
        List<Message> list = valueCommands.multiGet(NAMESPACE2, keys);
        assertEquals(3, list.size());
        assertEquals(message1, list.get(2));
    }

    @Test
    public void multiSetIfAbsent() {
        valueCommands.set(NAMESPACE1, key1, message2);
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        valueCommands.multiSetIfAbsent(NAMESPACE1, map);
        assertNotSame(message1, valueCommands.get(NAMESPACE1, key1));
        assertEquals(message2, valueCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void getAndSet() {
        valueCommands.set(NAMESPACE1, key1, message1);
        Message old = valueCommands.getAndSet(NAMESPACE1, key1, message2);
        assertEquals(message1, old);
        assertEquals(message2, valueCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void testCache() {
        valueCommands.set(NAMESPACE1, key1, message1);
        Message message = valueCommands.get(NAMESPACE1, key1, 5, TimeUnit.SECONDS);
        assertNotNull(message);
        message = valueCommands.get(NAMESPACE1, key1, 5, TimeUnit.SECONDS);
        assertNotNull(message);
        message = valueCommands.get(NAMESPACE1, key1, 5, TimeUnit.SECONDS);
        assertNotNull(message);
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
        }
        message = valueCommands.get(NAMESPACE1, key1, 5, TimeUnit.SECONDS);
        assertNotNull(message);

        valueCommands.set(NAMESPACE1, "echo", "test");
        String value = valueCommands.get(NAMESPACE1, "echo", 5, TimeUnit.SECONDS);
        assertNotNull(value);
        value = valueCommands.get(NAMESPACE1, "echo", 5, TimeUnit.SECONDS);
        assertNotNull(value);
        value = valueCommands.get(NAMESPACE1, "echo", 5, TimeUnit.SECONDS);
        assertNotNull(value);
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
        }
        value = valueCommands.get(NAMESPACE1, "echo", 5, TimeUnit.SECONDS);
        assertNotNull(value);
    }

}
