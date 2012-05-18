/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.ListCommands;
import com.taobao.common.tedis.group.TedisGroup;

public class ListCommandsTest extends BaseTestCase {

    private static ListCommands listCommands;
    private static final Key listkey1 = new Key(1000);
    private static final Key listkey2 = new Key(1001);
    private static final Key listkey3 = new Key(1002);
    private static final Key listkey4 = new Key(1003);

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        listCommands = new DefaultListCommands(tedisGroup.getTedis());
    }

    @Test
    public void leftPushPop() {
        long oldSize = listCommands.size(NAMESPACE1, listkey1);
        assertTrue(listCommands.leftPush(NAMESPACE1, listkey1, message1) ==  oldSize+ 1);
        assertEquals(message1, listCommands.leftPop(NAMESPACE1, listkey1));
    }

    @Test
    public void leftPushIfPresent() {
        long oldSize = listCommands.leftPush(NAMESPACE1, listkey1, message1);
        long size = listCommands.leftPushIfPresent(NAMESPACE1, listkey1, message2);
        assertEquals(size, oldSize + 1);
    }

    @Test
    public void range() {
        listCommands.leftPush(NAMESPACE1, listkey1, message1);
        listCommands.leftPush(NAMESPACE1, listkey1, message1);
        listCommands.leftPush(NAMESPACE1, listkey1, message1);
        listCommands.leftPush(NAMESPACE1, listkey1, message1);
        List<Message> list = listCommands.range(NAMESPACE1, listkey1, 0, 3);
        assertNotNull(list);
        assertTrue(list.size() == 4);
        assertEquals(message1, list.get(3));
    }

    @Test
    public void remove() {
        listCommands.leftPush(NAMESPACE2, listkey2, message2);
        long removed = listCommands.remove(NAMESPACE2, listkey2, 1, message2);
        assertTrue(removed == 1);
    }

    @Test
    public void rightPushPop() {
    	System.out.println(listCommands.size(NAMESPACE3, listkey3));
        long size1 = listCommands.rightPush(NAMESPACE3, listkey3, message3);
        System.out.println("size1:" +size1);
        System.out.println(listCommands.size(NAMESPACE3, listkey3));
        Message message = listCommands.rightPop(NAMESPACE3, listkey3);
        System.out.println(listCommands.size(NAMESPACE3, listkey3));
        assertEquals(message3, message);
        assertTrue(size1 == listCommands.size(NAMESPACE3, listkey3) + 1);
    }

    @Test
    public void rightPopAndLeftPush() {
        listCommands.rightPush(NAMESPACE2, listkey3, message1);
        listCommands.rightPopAndLeftPush(NAMESPACE2, listkey3, listkey4);
        assertEquals(message1, listCommands.index(NAMESPACE2, listkey4, 0));
    }

    @Test
    public void rightPushIfPresent() {
        long oldSize = listCommands.rightPush(NAMESPACE1, listkey1, message1);
        long size = listCommands.rightPushIfPresent(NAMESPACE1, listkey1, message2);
        assertEquals(size, oldSize + 1);
    }

    @Test
    public void set() {
        listCommands.leftPush(NAMESPACE1, listkey4, message1, message2, message3);
        listCommands.set(NAMESPACE1, listkey4, 1, message3);
        listCommands.set(NAMESPACE1, listkey4, 2, message1);
        listCommands.set(NAMESPACE1, listkey4, 3, message2);
        Message message = listCommands.index(NAMESPACE1, listkey4, 0);
        assertEquals(message3, message);
        assertEquals(message3, listCommands.index(NAMESPACE1, listkey4, 1));
        assertEquals(message1, listCommands.index(NAMESPACE1, listkey4, 2));
        assertEquals(message2, listCommands.index(NAMESPACE1, listkey4, 3));
    }

    @Test
    public void trim() {
        listCommands.leftPush(NAMESPACE1, listkey4, message1, message2, message3, message1);
        listCommands.leftPush(NAMESPACE1, listkey4, message1);
        listCommands.set(NAMESPACE1, listkey4, 4, message3);
        listCommands.trim(NAMESPACE1, listkey4, 0, 0);
        assertTrue(1 ==  listCommands.size(NAMESPACE1, listkey4));
    }


}
