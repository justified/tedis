/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.cache;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalCacheTest {

    LocalCache<String, String> cache;

    @Before
    public void setUpBeforeClass() throws Exception {
        cache = new DefaultLocalCache<String, String>();
    }

    @After
    public void tearDownAfterClass() throws Exception {
        cache.clear();
    }

    @Test
    public void testGet() {
        cache.put("key1", "value1");

        Assert.assertEquals("value1", cache.get("key1"));
    }

    @Test
    public void testClear() {
        cache.put("key1", "value1");
        Assert.assertEquals("value1", cache.get("key1"));

        cache.clear();

        Assert.assertNull(cache.get("key1"));
    }

    @Test
    public void testContainsKey() {
        cache.put("key1", "value1");
        Assert.assertTrue(cache.containsKey("key1"));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testKeySet() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        Set keys = cache.keySet();

        Assert.assertTrue(keys.contains("key1"));
        Assert.assertTrue(keys.contains("key2"));
        Assert.assertTrue(keys.contains("key3"));
    }

    @Test
    public void testPutObjectObject() {
        cache.put("key1", "value1");
        Assert.assertEquals("value1", cache.get("key1"));
    }

    @Test
    public void testRemove() {
        cache.put("key1", "value1");
        Assert.assertEquals("value1", cache.get("key1"));

        cache.remove("key1");
        Assert.assertNull(cache.get("key1"));
    }

    @Test
    public void testSize() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key3", "value4");

        Assert.assertEquals(3, cache.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testValues() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key3", "value4");

        Collection values = cache.values();

        Assert.assertTrue(values.contains("value1"));
        Assert.assertTrue(values.contains("value2"));
        Assert.assertTrue(values.contains("value4"));
        Assert.assertFalse(values.contains("value3"));
    }

    @Test
    public void testPutStringObjectDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.SECOND, 5);

        cache.put("key1", "value1", calendar.getTime());
        Assert.assertEquals("value1", cache.get("key1"));

        calendar.add(Calendar.SECOND, 10);

        cache.put("key2", "value2", calendar.getTime());
        Assert.assertEquals("value2", cache.get("key2"));

        try {
            Thread.sleep(8 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNull(cache.get("key1"));

        Assert.assertNotNull(cache.get("key2"));

    }

}
