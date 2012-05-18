/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.filter;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.event.ReplDBMSEvent;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

public interface Filter extends ReplicatorPlugin {
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException, InterruptedException;
}
