/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.plugin;

import java.util.List;

import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.conf.FailurePolicy;
import com.taobao.common.tedis.replicator.statemachine.event.EventDispatcher;
import com.taobao.common.tedis.replicator.storage.Store;

public interface PluginContext {
    /**
     * Returns the current replicator properties.
     */
    public ReplicatorProperties getReplicatorProperties();

    /** Returns a JDBC URL suitable for login to local data source. */
    public String getJdbcUrl(String database);

    /** Returns a user for login to local data source. */
    public String getJdbcUser();

    /** Returns a password suitable for login to local data source. */
    public String getJdbcPassword();

    /** Schema name for storing replicator catalogs. */
    public String getReplicatorSchemaName();

    /** Returns the applier failure policy. */
    public abstract FailurePolicy getApplierFailurePolicy();

    /** Returns the extractorFailurePolicy value. */
    public abstract FailurePolicy getExtractorFailurePolicy();

    /** Source ID for this replicator. */
    public abstract String getSourceId();

    /** Service name to which replication belongs. */
    public abstract String getServiceName();

    /** Returns true if replicator should go on-line automatically. */
    public abstract boolean isAutoEnable();

    /** Returns a named storage component. */
    public abstract Store getStore(String name);

    /** Returns all stores. */
    public abstract List<Store> getStores();

    /** Returns the event dispatcher for reporting interesting events. */
    public EventDispatcher getEventDispatcher();

    /** Returns the named extension or null if the extension does not exist. */
    public ReplicatorPlugin getExtension(String name);

    /** Returns the current list of extensions. */
    public List<String> getExtensionNames();

    /**
     * Returns true if we want to log replicator updates. This is equivalent to
     * MySQL's log_slave_updates option.
     */
    public boolean logReplicatorUpdates();

}