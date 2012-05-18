/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

/**
 * Implements an in-memory queue store with multiple queues. This is used for
 * testing other parallel queues where we need to simulate ability to apply in
 * parallel.
 */
public class InMemoryMultiQueue implements Store {
    private static Logger logger = Logger.getLogger(InMemoryMultiQueue.class);
    private String name;
    private int partitions = 1;
    private int maxSize = 1;

    private List<BlockingQueue<ReplDBMSEvent>> queues;
    private ReplDBMSHeader[] lastHeader;
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

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(int taskId, ReplDBMSHeader header) {
        lastHeader[taskId] = header;
    }

    /** Returns the last header processed. */
    public ReplDBMSHeader getLastHeader(int taskId) {
        return lastHeader[taskId];
    }

    public long getMaxStoredSeqno() {
        return -1;
    }

    public long getMinStoredSeqno() {
        return 0;
    }

    /**
     * Puts an event in the queue, blocking if it is full.
     */
    public void put(int taskId, ReplDBMSEvent event) throws InterruptedException {
        queues.get(taskId).put(event);
        transactionCount++;
        if (logger.isDebugEnabled()) {
            if (transactionCount % 10000 == 0)
                logger.debug("Queue store: xacts=" + transactionCount);
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplDBMSEvent get(int taskId) throws InterruptedException {
        return queues.get(taskId).take();
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplDBMSEvent peek(int taskId) {
        return queues.get(taskId).peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size(int taskId) {
        return queues.get(taskId).size();
    }

    public void configure(PluginContext context) throws ReplicatorException {
        // Nothing to do.
    }

    public void prepare(PluginContext context) throws ReplicatorException {
        queues = new ArrayList<BlockingQueue<ReplDBMSEvent>>(partitions);
        for (int i = 0; i < partitions; i++) {
            queues.add(new LinkedBlockingQueue<ReplDBMSEvent>(maxSize));
        }
        lastHeader = new ReplDBMSHeader[partitions];
    }

    public void release(PluginContext context) throws ReplicatorException {
        queues = null;
    }

    public ReplicatorProperties status() {
        ReplicatorProperties props = new ReplicatorProperties();
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", this.transactionCount);
        return props;
    }

    public long getMaxCommittedSeqno() {
        // No committed seqno into the queue store
        return -1;
    }
}