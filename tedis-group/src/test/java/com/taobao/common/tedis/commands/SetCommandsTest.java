/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.SetCommands;
import com.taobao.common.tedis.group.TedisGroup;

public class SetCommandsTest extends BaseTestCase {

    private static SetCommands setCommands;

    @BeforeClass
    public static void setUp() throws Exception {
        Group tedisGroup = new TedisGroup(appName, version);
        tedisGroup.init();
        setCommands = new DefaultSetCommands(tedisGroup.getTedis());
        tedisGroup.getTedis().flushAll();
    }

    @Test
    public void difference(){
        setCommands.add(NAMESPACE1, key1, message1, message2, message3);
        setCommands.remove(NAMESPACE1, key2, message1);
        Set<Message> sets = setCommands.difference(NAMESPACE1, key1, key2);
        assertTrue(sets.contains(message1));
    }

    @Test
    public void differenceAndStore(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key2, message2);
        setCommands.add(NAMESPACE1, key2, message3);
        setCommands.remove(NAMESPACE1, key2, message1);
        setCommands.differenceAndStore(NAMESPACE1, key1, key2, key3);
        Set<Message> sets = setCommands.members(NAMESPACE1, key3);
        assertTrue(sets.contains(message1));
    }

    @Test
    public void intersect(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key2, message2);
        setCommands.add(NAMESPACE1, key2, message3);
        Set<Message> sets = setCommands.intersect(NAMESPACE1, key1, key2);
        assertTrue(sets.contains(message2));
    }

    @Test
    public void intersectAndStore(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key2, message2);
        setCommands.add(NAMESPACE1, key2, message3);
        setCommands.intersectAndStore(NAMESPACE1, key1, key2, key3);
        Set<Message> sets = setCommands.members(NAMESPACE1, key3);
        assertTrue(sets.contains(message2));
    }

    @Test
    public void union(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key2, message2);
        setCommands.add(NAMESPACE1, key2, message3);
        Set<Message> sets = setCommands.union(NAMESPACE1, key1, key2);
        assertTrue(sets.contains(message1));
        assertTrue(sets.contains(message2));
        assertTrue(sets.contains(message3));
    }

    @Test
    public void unionAndStore(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key2, message2);
        setCommands.add(NAMESPACE1, key2, message3);
        setCommands.unionAndStore(NAMESPACE1, key1, key2, key3);
        Set<Message> sets = setCommands.members(NAMESPACE1, key3);
        assertTrue(sets.contains(message1));
        assertTrue(sets.contains(message2));
        assertTrue(sets.contains(message3));
    }

    @Test
    public void add(){
        setCommands.add(NAMESPACE1, key1, message1, message2);
        setCommands.remove(NAMESPACE1, key1, message1, message2, message3);
        assertEquals(new Long(3), setCommands.add(NAMESPACE1, key1, message1, message2, message3));
    }

    @Test
    public void isMember(){
        setCommands.add(NAMESPACE1, key1, message1);
        assertTrue(setCommands.isMember(NAMESPACE1, key1, message1));
    }

    @Test
    public void members(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key1, message3);
        Set<Message> sets  = setCommands.members(NAMESPACE1, key1);
        assertTrue(sets.contains(message1));
        assertTrue(sets.contains(message2));
        assertTrue(sets.contains(message3));
    }

    @Test
    public void move(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.move(NAMESPACE1, key1, message1, key2);
        assertFalse(setCommands.members(NAMESPACE1, key1).contains(message1));
        assertTrue(setCommands.members(NAMESPACE1, key2).contains(message1));
    }

    @Test
    public void randomMember(){
        setCommands.add(NAMESPACE1, key1, message1);
        setCommands.add(NAMESPACE1, key1, message2);
        setCommands.add(NAMESPACE1, key1, message3);
        assertNotNull(setCommands.randomMember(NAMESPACE1, key1));
    }

    @Test
    public void remove(){
        setCommands.remove(NAMESPACE1, key1, message1);
        assertFalse(setCommands.members(NAMESPACE1, key1).contains(message1));
    }

    @Test
    public void pop(){
        setCommands.add(NAMESPACE1, key1, message1);
        assertNotNull(setCommands.pop(NAMESPACE1, key1));
    }

    @Test
    public void size(){
        setCommands.remove(NAMESPACE1, key1, message1);
        long old = setCommands.size(NAMESPACE1, key1);
        setCommands.add(NAMESPACE1, key1, message1);
        assertEquals(old + 1, setCommands.size(NAMESPACE1, key1).longValue());
    }

}
