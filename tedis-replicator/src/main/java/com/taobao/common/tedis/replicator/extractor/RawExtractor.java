/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.extractor;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.DBMSEvent;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

public interface RawExtractor extends ReplicatorPlugin {
    public void setLastEventId(String eventId) throws ReplicatorException;

    public DBMSEvent extract() throws ReplicatorException, InterruptedException;

    public DBMSEvent extract(String eventId) throws ReplicatorException, InterruptedException;

    public String getCurrentResourceEventId() throws ReplicatorException, InterruptedException;
}
