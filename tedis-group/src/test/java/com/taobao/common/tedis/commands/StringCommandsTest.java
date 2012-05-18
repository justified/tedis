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
import com.taobao.common.tedis.core.StringCommands;
import com.taobao.common.tedis.group.TedisGroup;

public class StringCommandsTest extends BaseTestCase {

    private static StringCommands stringCommands;
    String key1 = "test key 1";
    String key2 = "test key 2";
    String key3 = "test key 3";
    String message1 = "test string 1";
    String message2 = "test string 2";
    String message3 = "test string 3";

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        stringCommands = new DefaultStringCommands(tedisGroup.getTedis());
        tedisGroup.getTedis().flushAll();
    }

    @Test
    public void setGetTest() {
        stringCommands.set(NAMESPACE1, key1, message1);
        assertNotNull(stringCommands.get(NAMESPACE1, key1));
        assertEquals(message1, stringCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void msetTest() {
        Map<String, String> map = new HashMap<String, String>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        stringCommands.multiSet(NAMESPACE2, map);
        assertEquals(message1, stringCommands.get(NAMESPACE2, key1));
        assertEquals(message2, stringCommands.get(NAMESPACE2, key2));
        assertEquals(message3, stringCommands.get(NAMESPACE2, key3));
    }

    @Test
    public void mgetTest() {
        Map<String, String> map = new HashMap<String, String>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        stringCommands.multiSet(NAMESPACE2, map);
        Collection<String> keys = new ArrayList<String>();
        keys.add(key2);
        keys.add(key3);
        keys.add(key1);
        List<String> list = stringCommands.multiGet(NAMESPACE2, keys);
        assertEquals(3, list.size());
        assertEquals(message1, list.get(2));
    }

    @Test
    public void multiSetIfAbsent() {
        stringCommands.set(NAMESPACE1, key1, message2);
        Map<String, String> map = new HashMap<String, String>();
        map.put(key1, message1);
        map.put(key2, message2);
        map.put(key3, message3);
        stringCommands.multiSetIfAbsent(NAMESPACE1, map);
        assertNotSame(message1, stringCommands.get(NAMESPACE1, key1));
        assertEquals(message2, stringCommands.get(NAMESPACE1, key1));
        assertNull(stringCommands.get(NAMESPACE1, key2));
    }

    @Test
    public void getAndSet() {
        stringCommands.set(NAMESPACE1, key1, message1);
        String old = stringCommands.getAndSet(NAMESPACE1, key1, message2);
        assertEquals(message1, old);
        assertEquals(message2, stringCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void append(){
        String appended = "appended";
        stringCommands.set(NAMESPACE3, key3, "something");
        stringCommands.append(NAMESPACE3, key3, appended);
        assertTrue(stringCommands.get(NAMESPACE3, key3).endsWith(appended));
    }

    @Test
    public void getRange(){
        String source = "Hello World";
        stringCommands.set(NAMESPACE1, key1, source);
        assertEquals("World", stringCommands.get(NAMESPACE1, key1, 6, 11));
    }

    @Test
    public void setOffset(){
        String source = "Hello World";
        stringCommands.set(NAMESPACE1, key1, source);
        stringCommands.set(NAMESPACE1, key1, "Redis", 6);
        assertEquals("Hello Redis", stringCommands.get(NAMESPACE1, key1));
    }

    @Test
    public void size(){
        String source = "Hello World";
        stringCommands.set(NAMESPACE1, key1, source);
        assertTrue(11 == stringCommands.size(NAMESPACE1, key1));
    }

}
