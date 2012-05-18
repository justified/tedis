/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tedis.Group;
import com.taobao.common.tedis.core.ZSetCommands;
import com.taobao.common.tedis.group.TedisGroup;

public class ZSetCommandsTest extends BaseTestCase {

	private static ZSetCommands zsetCommands;

	@BeforeClass
	public static void setUp() throws Exception {
		Group tedisGroup = new TedisGroup(appName, version);
		tedisGroup.init();
		zsetCommands = new DefaultZSetCommands(tedisGroup.getTedis());
		tedisGroup.getTedis().flushAll();
	}

	@Test
	public void intersectAndStore() {
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		zsetCommands.add(NAMESPACE1, key1, message2, 2);
		zsetCommands.add(NAMESPACE1, key2, message2, 2);
		zsetCommands.add(NAMESPACE1, key2, message3, 3);
		zsetCommands.intersectAndStore(NAMESPACE1, key1, key2, key3);
		Set<Message> sets = zsetCommands.range(NAMESPACE1, key3, 0, 2);
		assertTrue(sets.contains(message2));
		assertFalse(sets.contains(message1));
		assertFalse(sets.contains(message3));
	}

	@Test
	public void unionAndStore() {
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		zsetCommands.add(NAMESPACE1, key1, message2, 2);
		zsetCommands.add(NAMESPACE1, key2, message2, 2);
		zsetCommands.add(NAMESPACE1, key2, message3, 3);
		zsetCommands.unionAndStore(NAMESPACE1, key1, key2, key3);
		Set<Message> sets = zsetCommands.range(NAMESPACE1, key3, 0, 2);
		assertTrue(sets.contains(message1));
		assertTrue(sets.contains(message2));
		assertTrue(sets.contains(message3));
	}

	@Test
	public void add() {
		zsetCommands.remove(NAMESPACE1, key1, message1, message2, message3);
		Map<Message, Double> values = new HashMap<BaseTestCase.Message, Double>();
		values.put(message1, 1.0);
		values.put(message2, 2.0);
		assertEquals(new Long(2), zsetCommands.add(NAMESPACE1, key1, values));
	}

	@Test
	public void incrementScore() {
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		assertEquals(2.0, zsetCommands.incrementScore(NAMESPACE1, key1, message1, 1), 1);
	}

	@Test
	public void rank() {
		zsetCommands.add(NAMESPACE1, key1, message3, 3);
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		zsetCommands.add(NAMESPACE1, key1, message2, 2);
		long rank = zsetCommands.rank(NAMESPACE1, key1, message1);
		assertEquals(0, rank);
	}

	@Test
	public void reverseRank() {
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		zsetCommands.add(NAMESPACE1, key1, message2, 2);
		zsetCommands.add(NAMESPACE1, key1, message3, 3);
		long rank = zsetCommands.reverseRank(NAMESPACE1, key1, message1);
		assertEquals(2, rank);
	}

	@Test
	public void score() {
		zsetCommands.add(NAMESPACE1, key1, message1, 2);
		assertEquals(2.0, zsetCommands.score(NAMESPACE1, key1, message1).doubleValue(), 1);
	}

	@Test
	public void remove() {
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		assertEquals(new Long(1), zsetCommands.remove(NAMESPACE1, key1, message1));
	}

	@Test
	public void removeRange() {
		zsetCommands.add(NAMESPACE1, key3, message1, 1);
		zsetCommands.add(NAMESPACE1, key3, message2, 2);
		zsetCommands.add(NAMESPACE1, key3, message3, 3);
		zsetCommands.removeRange(NAMESPACE1, key3, 1, 2);
		Set<Message> sets = zsetCommands.range(NAMESPACE1, key3, 1, 2);
		assertEquals(0, sets.size());
	}

	@Test
	public void rangeWithScore() {
		zsetCommands.removeRange(NAMESPACE1, key3, 0, -1);
		zsetCommands.add(NAMESPACE1, key3, message2, 2);
		zsetCommands.add(NAMESPACE1, key3, message1, 1);
		zsetCommands.add(NAMESPACE1, key3, message3, 3);
		Map<Message, Double> sets = zsetCommands.rangeWithScore(NAMESPACE1, key3, 0, 2);
		for (Map.Entry<Message, Double> entry : sets.entrySet()) {
			System.out.println(entry.getKey() + " score:" + entry.getValue());
		}
		assertEquals((Double) 1.0, sets.get(message1));
	}

	@Test
	public void reverseRangeWithScore() {
		zsetCommands.removeRange(NAMESPACE1, key3, 0, -1);
		zsetCommands.add(NAMESPACE1, key3, message2, 2);
		zsetCommands.add(NAMESPACE1, key3, message1, 1);
		zsetCommands.add(NAMESPACE1, key3, message3, 3);
		Map<Message, Double> sets = zsetCommands.reverseRangeWithScore(NAMESPACE1, key3, 0, 2);
		for (Map.Entry<Message, Double> entry : sets.entrySet()) {
			System.out.println(entry.getKey() + " score:" + entry.getValue());
		}
		assertEquals((Double) 1.0, sets.get(message1));
	}

	@Test
	public void rangeByScoreWithScore() {
		zsetCommands.removeRange(NAMESPACE1, key3, 0, -1);
		zsetCommands.add(NAMESPACE1, key3, message2, 2);
		zsetCommands.add(NAMESPACE1, key3, message1, 1);
		zsetCommands.add(NAMESPACE1, key3, message3, 3);
		Map<Message, Double> sets = zsetCommands.rangeByScoreWithScore(NAMESPACE1, key3, 1, 3);
		for (Map.Entry<Message, Double> entry : sets.entrySet()) {
			System.out.println(entry.getKey() + " score:" + entry.getValue());
		}
		assertEquals((Double) 1.0, sets.get(message1));
	}

	@Test
	public void removeRangeByScore() {
		zsetCommands.add(NAMESPACE1, key3, message1, 1);
		zsetCommands.add(NAMESPACE1, key3, message2, 2);
		zsetCommands.add(NAMESPACE1, key3, message3, 3);
		zsetCommands.removeRangeByScore(NAMESPACE1, key3, 2, 3);
		Set<Message> sets = zsetCommands.rangeByScore(NAMESPACE1, key3, 2, 3);
		assertEquals(0, sets.size());
	}

	@Test
	public void count() {
		zsetCommands.add(NAMESPACE3, key3, message1, 1);
		zsetCommands.add(NAMESPACE3, key3, message2, 2);
		zsetCommands.add(NAMESPACE3, key3, message3, 3);
		long count = zsetCommands.count(NAMESPACE3, key3, 2, 3);
		assertEquals(2, count);
	}

	@Test
	public void size() {
		zsetCommands.remove(NAMESPACE1, key1, message1);
		long old = zsetCommands.size(NAMESPACE1, key1);
		zsetCommands.add(NAMESPACE1, key1, message1, 1);
		assertEquals(old + 1, zsetCommands.size(NAMESPACE1, key1).longValue());
	}
}
