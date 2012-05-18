/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.binary.DataType;
import com.taobao.common.tedis.group.TedisGroup;
import com.taobao.common.tedis.util.SortParams;

public class TedisCommandsTest extends BaseTestCase {

    private static DefaultTedisManager tedisManager;

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        tedisManager = new DefaultTedisManager(tedisGroup);
//        tedisManager.setKeySerializer(new StringTedisSerializer());
//        tedisManager.setValueSerializer(new StringTedisSerializer());
//        tedisManager.setHashKeySerializer(new StringTedisSerializer());
        tedisGroup.getTedis().flushAll();
    }

    @Test
    public void hasKey() {
        assertFalse(tedisManager.hasKey(NAMESPACE1, key1));
        tedisManager.getValueCommands().set(NAMESPACE1, key1, message1);
        assertTrue(tedisManager.hasKey(NAMESPACE1, key1));
    }

    @Test
    public void delete() {
        tedisManager.delete(NAMESPACE1, key1);
        assertFalse(tedisManager.hasKey(NAMESPACE1, key1));
    }

    @Test
    public void type() {
        tedisManager.getValueCommands().set(NAMESPACE1, key1, message1);
        assertEquals(DataType.VALUE, tedisManager.type(NAMESPACE1, key1));
        tedisManager.getListCommands().leftPush(NAMESPACE2, key2, message2);
        assertEquals(DataType.LIST, tedisManager.type(NAMESPACE2, key2));
    }

    @Test
    public void keys() {
        tedisManager.getValueCommands().set(NAMESPACE1, key1, message1);
        System.out.println(key1);
        Set<Key> keys = tedisManager.keys(NAMESPACE1, "*");
        System.out.println(keys);
    }

    @Test
    public void rename() {
        tedisManager.getValueCommands().set(NAMESPACE1, key1, message1);
        tedisManager.rename(NAMESPACE1, key1, key2);
    }

    @Test
    public void renameIfAbsent() {
        tedisManager.renameIfAbsent(NAMESPACE1, key1, key2);
    }

    @Test
    public void expire() {
        tedisManager.expire(NAMESPACE1, key1, 5, TimeUnit.SECONDS);
    }

    @Test
    public void expireAt() {
        tedisManager.expireAt(NAMESPACE1, key1, new Date());
    }

    @Test
    public void persist() {
        tedisManager.persist(NAMESPACE1, key1);
    }

    @Test
    public void getExpire() {
        tedisManager.getExpire(NAMESPACE1, key1);
    }

    @Test
    public void sort() {
        tedisManager.getListCommands().leftPush(NAMESPACE1, key1, message1);
        tedisManager.getListCommands().leftPush(NAMESPACE1, key1, message2);
        tedisManager.getListCommands().leftPush(NAMESPACE1, key1, message3);
        List<Message> messages = tedisManager.sort(NAMESPACE1, key1, new SortParams().desc().limit(0, 1));
        assertEquals(message3, messages.get(0));
    }

    @Test
    public void sortby() {
        tedisManager.getListCommands().leftPush(41, "key1", "1");
        tedisManager.getListCommands().leftPush(41, "key1", "2");
        tedisManager.getListCommands().leftPush(41, "key1", "3");

        // tedisManager.getSetCommands().add(41, "key1", "set1");
        // tedisManager.getSetCommands().add(41, "key1", "set2");
        // tedisManager.getSetCommands().add(41, "key1", "set3");

        tedisManager.getAtomicCommands().set(41, "sort_1", 4);
        tedisManager.getAtomicCommands().set(41, "sort_2", 5);
        tedisManager.getAtomicCommands().set(41, "sort_3", 1);

        System.out.println("sort:" + tedisManager.sort(41, "key1", new SortParams().by(41, "sort_*").asc()));
    }

}
