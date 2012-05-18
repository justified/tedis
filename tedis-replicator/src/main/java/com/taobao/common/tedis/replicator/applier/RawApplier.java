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
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

public interface RawApplier extends ReplicatorPlugin {
    /**
     * Sets the ID of the task using this raw applier.
     *
     * @param id
     *            Task ID
     */
    public void setTaskId(int id);

    /**
     * Apply the proffered event to the replication target.
     *
     * @param event
     *            Event to be applied
     * @param header
     *            Header data corresponding to event
     * @param doCommit
     *            Boolean flag indicating whether this is the last part of
     *            multi-part event
     * @param doRollback
     *            Boolean flag indicating whether this transaction should
     *            rollback
     * @throws ReplicatorException
     *             Thrown if applier processing fails
     * @throws ConsistencyException
     *             Thrown if the applier detects that a consistency check has
     *             failed
     * @throws InterruptedException
     *             Thrown if the applier is interrupted
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException;

    /**
     * Commits current open transaction to ensure data applied up to current
     * point are durable.
     *
     * @throws ReplicatorException
     *             Thrown if applier processing fails
     * @throws InterruptedException
     *             Thrown if the applier is interrupted
     */
    public void commit() throws ReplicatorException, InterruptedException;

    /**
     * Rolls back any current work.
     *
     * @throws InterruptedException
     *             Thrown if the applier is interrupted.
     */
    public void rollback() throws InterruptedException;

    /**
     * Return header information corresponding to last committed event.
     *
     * @return Header data for last committed event.
     * @throws ReplicatorException
     *             Thrown if getting sequence number fails
     * @throws InterruptedException
     *             Thrown if the applier is interrupted
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException;
}
