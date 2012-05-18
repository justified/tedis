/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.DBMSEmptyEvent;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.event.ReplDBMSHeader;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

/**
 * This class wraps a basic Applier so that it handles ReplDBMSEvent values with
 * assigned sequence numbers.
 */
public class ApplierWrapper implements Applier {
    private static Logger logger = Logger.getLogger(ApplierWrapper.class);
    private RawApplier applier;

    public ApplierWrapper(RawApplier applier) {
        this.applier = applier;
    }

    public RawApplier getApplier() {
        return applier;
    }

    public void setTaskId(int id) {
        applier.setTaskId(id);
    }

    public void apply(ReplDBMSEvent event, boolean doCommit, boolean doRollback) throws ReplicatorException, InterruptedException {
        DBMSEvent myEvent = event.getDBMSEvent();
        if (myEvent instanceof DBMSEmptyEvent) {
            // Handling empty events :
            // - if it is the first fragment, this is an empty
            // commit, it can then be safely ignored
            // - if it is the last fragment, it should commit
            if (event.getFragno() > 0) {
                applier.apply(myEvent, event, true, false);
            } else {
                // Empty commit : just ignore
                applier.apply(myEvent, event, false, false);
            }
        } else
            applier.apply(myEvent, event, doCommit, doRollback);
    }

    public void updatePosition(ReplDBMSHeader header, boolean doCommit) throws ReplicatorException, InterruptedException {
        DBMSEmptyEvent empty = new DBMSEmptyEvent(null, null);
        applier.apply(empty, header, doCommit, false);
    }

    public void commit() throws ReplicatorException, InterruptedException {
        applier.commit();
    }

    public void rollback() throws InterruptedException {
        applier.rollback();
    }

    public ReplDBMSHeader getLastEvent() throws ReplicatorException, InterruptedException {
        return applier.getLastEvent();
    }

    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.debug("Configuring raw applier");
        applier.configure(context);
    }

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.debug("Preparing raw applier");
        applier.prepare(context);
    }

    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
        logger.debug("Releasing raw applier");
        applier.release(context);
    }
}
