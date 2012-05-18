/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntime;

public interface OpenReplicatorPlugin {
    public void prepare(ReplicatorContext context) throws ReplicatorException;

    public void release() throws ReplicatorException;

    public void configure(ReplicatorProperties properties) throws ReplicatorException;

    public void online(ReplicatorProperties params) throws Exception;

    public void offline(ReplicatorProperties params) throws Exception;

    public void offlineDeferred(ReplicatorProperties params) throws Exception;

    public String flush(long timeout) throws Exception;

    public boolean waitForAppliedEvent(String event, long timeout) throws Exception;

    public HashMap<String, String> status() throws Exception;

    public List<Map<String, String>> statusList(String name) throws Exception;

    public static final String STATUS_LAST_APPLIED = "last-applied";

    public static final String STATUS_LAST_RECEIVED = "last-received";

    public ReplicatorRuntime getReplicatorRuntime();
}
