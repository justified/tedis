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
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.HashCommands;
import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.group.TedisGroup;

public class HashCommandsTest extends BaseTestCase {

    private static HashCommands hashCommands;

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        TedisManager tedisManager = new DefaultTedisManager(tedisGroup);
        hashCommands = tedisManager.getHashCommands();
        tedisGroup.getTedis().flushAll();
    }

    @Test
    public void delete(){
        hashCommands.put(NAMESPACE1, key1, key1, message1);
        hashCommands.put(NAMESPACE1, key1, key2, message2);
        hashCommands.put(NAMESPACE1, key1, key3, message3);
        hashCommands.delete(NAMESPACE1, key1, key1,key3);
        assertNull(hashCommands.get(NAMESPACE1, key1, key1));
        assertNull(hashCommands.get(NAMESPACE1, key1, key3));
        assertEquals(message2, hashCommands.get(NAMESPACE1, key1, key2));
    }

    @Test
    public void hasKey(){
        hashCommands.put(NAMESPACE1, key1, key3, message3);
        assertTrue(hashCommands.hasKey(NAMESPACE1, key1, key3));
    }

    @Test
    public void multiGet(){
        hashCommands.put(NAMESPACE2, key1, key1, message1);
        hashCommands.put(NAMESPACE2, key1, key2, message2);
        hashCommands.put(NAMESPACE2, key1, key3, message3);
        Collection<Key> hashKeys = new ArrayList<Key>();
        hashKeys.add(key1);
        hashKeys.add(key2);
        hashKeys.add(key3);
        Message[] hashValues = new Message[]{message1, message2, message3};
        assertArrayEquals(hashValues, hashCommands.multiGet(NAMESPACE2, key1, hashKeys).toArray());
    }

    @Test
    public void increment(){
        long old = hashCommands.increment(NAMESPACE2, key2, key2, 0);
//        System.out.println(hashCommands.get(NAMESPACE2, key2, key2));
        assertEquals(old + 2, hashCommands.increment(NAMESPACE2, key2, key2, 2).longValue());
    }

    @Test
    public void  keys(){
        hashCommands.put(NAMESPACE3, key1, key1, message1);
        hashCommands.put(NAMESPACE3, key1, key2, message2);
        hashCommands.put(NAMESPACE3, key1, key3, message3);
        Set<Key> list = hashCommands.keys(NAMESPACE3, key1);
        assertTrue(list.contains(key1));
        assertTrue(list.contains(key2));
        assertTrue(list.contains(key3));
    }

    @Test
    public void  size(){
        long old = hashCommands.size(NAMESPACE1, key2);
        hashCommands.put(NAMESPACE1, key2, key1, message1);
        hashCommands.put(NAMESPACE1, key2, key2, message2);
        hashCommands.put(NAMESPACE1, key2, key3, message3);
        assertEquals(old + 3, hashCommands.size(NAMESPACE1, key2).longValue());
    }

    @Test
    public void  putAll(){
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        hashCommands.putAll(NAMESPACE1, key3, map);
        Collection<Key> hashKeys = new ArrayList<Key>();
        hashKeys.add(key1);
        hashKeys.add(key2);
        hashKeys.add(key3);
        Message[] hashValues = new Message[]{message1, message2, message3};
        assertArrayEquals(hashValues, hashCommands.multiGet(NAMESPACE1, key3, hashKeys).toArray());
    }

    @Test
    public void  putIfAbsent(){
        hashCommands.delete(NAMESPACE2, key3, key1);
        assertTrue(hashCommands.putIfAbsent(NAMESPACE2, key3, key1, message1));
        assertFalse(hashCommands.putIfAbsent(NAMESPACE2, key3, key1, message1));
    }

    @Test
    public void  values(){
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        hashCommands.putAll(NAMESPACE3, key3, map);
        Collection<Message> list = hashCommands.values(NAMESPACE3, key3);
        assertTrue(list.contains(message1));
        assertTrue(list.contains(message2));
        assertTrue(list.contains(message3));
    }

    @Test
    public void  entries(){
        Map<Key, Message> map = new HashMap<Key, Message>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        hashCommands.putAll(NAMESPACE3, key2, map);
        Map<Key, Message> m = hashCommands.entries(NAMESPACE3, key2);
        assertEquals(message1, m.get(key1));
        assertEquals(message2, m.get(key2));
        assertEquals(message3, m.get(key3));
    }
}
