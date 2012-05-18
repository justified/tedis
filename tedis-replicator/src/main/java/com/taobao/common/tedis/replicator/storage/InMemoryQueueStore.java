/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.storage;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class InMemoryQueueStore implements Store {
    private static Logger logger = Logger.getLogger(InMemoryQueueStore.class);
    private String name;
    private LinkedBlockingQueue<ReplDBMSEvent> queue;
    private int maxSize = 1;
    private ReplDBMSHeader lastHeader;
    private long transactionCount = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int size) {
        this.maxSize = size;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(ReplDBMSHeader header) {
        lastHeader = header;
    }

    /** Returns the last header processed. */
    public ReplDBMSHeader getLastHeader() {
        return lastHeader;
    }

    public long getMaxStoredSeqno() {
        return -1;
    }

    public long getMinStoredSeqno() {
        return 0;
    }

    public ReplDBMSEvent fetchEvent(long seqno, short fragno, boolean ignoreSkippedEvent) {
        // We don't keep events persistently, so this is always null.
        return null;
    }

    /**
     * Puts an event in the queue, blocking if it is full.
     */
    public void put(ReplDBMSEvent event) throws InterruptedException {
        queue.put(event);
        transactionCount++;
        if (logger.isDebugEnabled()) {
            if (transactionCount % 10000 == 0)
                logger.debug("Queue store: xacts=" + transactionCount + " size=" + queue.size());
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplDBMSEvent get() throws InterruptedException {
        return queue.take();
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplDBMSEvent peek() {
        return queue.peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size() {
        return queue.size();
    }

    public void configure(PluginContext context) throws ReplicatorException {
        // Nothing to do.
    }

    public void prepare(PluginContext context) throws ReplicatorException {
        queue = new LinkedBlockingQueue<ReplDBMSEvent>(maxSize);
    }

    public void release(PluginContext context) throws ReplicatorException {
        queue = null;
    }

    @Override
    public ReplicatorProperties status() {
        ReplicatorProperties props = new ReplicatorProperties();
        if (queue != null)
            props.setLong("storeSize", queue.size());
        else
            props.setLong("storeSize", -1);
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", this.transactionCount);
        return props;
    }

    @Override
    public long getMaxCommittedSeqno() {
        // No committed seqno into the queue store
        return -1;
    }
}
