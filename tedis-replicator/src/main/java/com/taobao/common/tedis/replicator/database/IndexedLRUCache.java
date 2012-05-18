/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexedLRUCache<T> {
    // Hash index on cache nodes.
    private Map<String, CacheNode<T>> pmap = new HashMap<String, CacheNode<T>>();

    // Most and leads recently used node.
    private CacheNode<T> lruFirst;
    private CacheNode<T> lruLast;

    // Maximum size of the cache.
    private int capacity;

    // Call-back to release values.
    private CacheResourceManager<T> resourceManager;

    public IndexedLRUCache(int capacity, CacheResourceManager<T> resourceManager) {
        this.capacity = capacity;
        this.resourceManager = resourceManager;
    }

    public int size() {
        return pmap.size();
    }

    public void put(String key, T value) {
        // If there is a previous node, unlink and release it.
        CacheNode<T> old = pmap.get(key);
        if (old != null)
            remove(old);

        // If the index is at capacity, unlink the least recently used node.
        if (pmap.size() >= capacity)
            remove(lruLast);

        // Add the new node.
        CacheNode<T> node = new CacheNode<T>(key, value);
        add(node);
    }

    public T get(String key) {
        CacheNode<T> node = pmap.get(key);
        if (node == null)
            return null;
        else {
            // Relink node in the LRU.
            unlink(node);
            link(node);
            return node.get();
        }
    }

    public Set<String> keys() {
        Set<String> keys = new HashSet<String>();
        keys.addAll(pmap.keySet());
        return keys;
    }

    public List<T> lruValues() {
        ArrayList<T> lruValues = new ArrayList<T>(pmap.size());
        CacheNode<T> next = lruFirst;
        while (next != null) {
            lruValues.add(next.get());
            next = next.getAfter();
        }
        return lruValues;
    }

    public int invalidateAll() {
        int deleted = this.pmap.size();
        for (String key : keys())
            invalidate(key);
        return deleted;
    }

    public int invalidateByPrefix(String prefix) {
        int deleted = 0;
        for (String key : keys()) {
            if (key.startsWith(prefix)) {
                invalidate(key);
                deleted++;
            }
        }
        return deleted;
    }

    public int invalidate(String key) {
        CacheNode<T> node = pmap.get(key);
        if (node != null) {
            remove(node);
            return 1;
        } else
            return 0;
    }

    private void add(CacheNode<T> node) {
        pmap.put(node.getKey(), node);
        link(node);
    }

    private void remove(CacheNode<T> node) {
        pmap.remove(node.getKey());
        unlink(node);
        if (resourceManager != null)
            resourceManager.release(node.get());
    }

    private void link(CacheNode<T> node) {
        if (lruFirst == null) {
            node.setAfter(null);
            node.setBefore(null);
            lruFirst = node;
            lruLast = node;
        } else {
            lruFirst.setBefore(node);
            node.setAfter(lruFirst);
            node.setBefore(null);
            lruFirst = node;
        }
    }

    private void unlink(CacheNode<T> node) {
        CacheNode<T> before = node.getBefore();
        CacheNode<T> after = node.getAfter();

        if (before == null)
            lruFirst = after;
        else
            before.setAfter(after);

        if (after == null)
            lruLast = before;
        else
            after.setBefore(before);
    }
}