/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.redis;

import java.util.Map;
import java.util.concurrent.Executor;

import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.replicator.plugin.ReplicatorPlugin;

public interface RedisHandler extends ReplicatorPlugin {

    void insert(TedisManager tedisManager, String tableName, Map<String, Object> values);

    void update(TedisManager tedisManager, String tableName, Map<String, Object> old, Map<String, Object> values);

    void delete(TedisManager tedisManager, String tableName, Map<String, Object> old);
    
    /**
     * @return The tables name you interest in.
     * @throws RedisHandleException
     */
    String[] interest();
    
    Executor getExecutor();
}
