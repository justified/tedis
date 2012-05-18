/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.storage;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes a storage component that holds replication events.
 */
public interface Store extends ReplicatorPlugin {
    /** Gets the storage name. */
    public String getName();

    /** Sets the storage name. */
    public void setName(String name);

    /**
     * Returns the maximum stored sequence number.
     */
    public long getMaxStoredSeqno();

    /**
     * Returns the minimum stored sequence number.
     */
    public long getMinStoredSeqno();

    /**
     * Returns status information as a set of named properties.
     */
    public ReplicatorProperties status();

    /**
     * Returns the maximum committed sequence number. TODO: This should be the
     * same as the max stored seqno.
     *
     * @throws InterruptedException
     */
    public long getMaxCommittedSeqno() throws ReplicatorException;
}