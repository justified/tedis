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
import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.conf.DIYExecutor;
import com.taobao.common.tedis.replicator.plugin.PluginContext;

public class DummyRedisHandler implements RedisHandler {

    private int customNamespace = 0;

    public void setConfigNamespace(int configNamespace) {
        this.customNamespace = configNamespace;
    }

    @Override
    public void delete(TedisManager tedisManager, String tableName, Map<String, Object> old) {
        System.err.println("Delete data:" + old);
        tedisManager.delete(customNamespace, old.values());
    }

    @Override
    public void insert(TedisManager tedisManager, String tableName, Map<String, Object> values) {
        System.err.println("Insert data:" + values);
        tedisManager.getValueCommands().multiSet(customNamespace, values);
    }

    @Override
    public void update(TedisManager tedisManager, String tableName, Map<String, Object> old, Map<String, Object> values) {
        System.err.println("Update data:" + old + " to data:" + values);
        tedisManager.getValueCommands().multiSet(customNamespace, values);
    }


    @Override
    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
    }

    @Override
    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {

    }

    @Override
    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
    }

	@Override
	public String[] interest() {
		return new String[]{"test"};
	}

	@Override
	public Executor getExecutor() {
		return DIYExecutor.getInstance();
	}

}
