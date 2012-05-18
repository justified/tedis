/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

public interface Applier extends ReplicatorPlugin {
    public void apply(ReplDBMSEvent event, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException;

    public void updatePosition(ReplDBMSHeader header, boolean doCommit) throws ReplicatorException, InterruptedException;

    public void commit() throws ReplicatorException, InterruptedException;

    public void rollback() throws InterruptedException;

    public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException;
}
