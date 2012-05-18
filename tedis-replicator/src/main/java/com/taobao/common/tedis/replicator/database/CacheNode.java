/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.database;

public class CacheNode<T> {

    // Value we are storing.
    private String key;
    private long lastAccessMillis;
    private T value;

    // Previous and after nodes in the LRU list.
    private CacheNode<T> before;
    private CacheNode<T> after;

    /** Create node and set initial access time. */
    public CacheNode(String key, T value) {
        this.key = key;
        this.value = value;
        this.lastAccessMillis = System.currentTimeMillis();
    }

    /**
     * Release resources associated with the value. Must be overridden by
     * clients to implement type-specific resource management. The node is
     * unusable after this call.
     */
    public void release() {
        value = null;
    }

    /* Returns the key to this node. */
    public String getKey() {
        return key;
    }

    /** Return the node value. */
    public T get() {
        lastAccessMillis = System.currentTimeMillis();
        return value;
    }

    /** Returns time of last access. */
    public long getLastAccessMillis() {
        return lastAccessMillis;
    }

    /** Return the before (newer) node or null in LRU list. */
    public CacheNode<T> getBefore() {
        return before;
    }

    /** Set the before node in the LRU list. */
    public void setBefore(CacheNode<T> previous) {
        this.before = previous;
    }

    /** Return the after (older) node in the LRU list. */
    public CacheNode<T> getAfter() {
        return after;
    }

    /** Set the after node in the LRU list. */
    public void setAfter(CacheNode<T> next) {
        this.after = next;
    }
}