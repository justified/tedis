package com.taobao.common.tedis.replicator.redis

import java.util.Map
import java.util.concurrent.Executor

import com.taobao.common.tedis.core.TedisManager
import com.taobao.common.tedis.replicator.ReplicatorException
import com.taobao.common.tedis.replicator.conf.DIYExecutor
import com.taobao.common.tedis.replicator.plugin.PluginContext
import com.taobao.common.tedis.replicator.redis.RedisHandler

class DummyHandler implements RedisHandler {

    def customNamespace = 0

    public void setConfigNamespace(int configNamespace) {
        this.customNamespace = configNamespace;
    }

    public void delete(TedisManager tedisManager, String tableName, Map<String, Object> old) {
        println "Delete data:" + old
        tedisManager.delete(customNamespace, old.values())
    }

    public void insert(TedisManager tedisManager, String tableName, Map<String, Object> values) {
        println "Insert data:" + values
        tedisManager.getValueCommands().multiSet(customNamespace, values)
    }

    public void update(TedisManager tedisManager, String tableName, Map<String, Object> old, Map<String, Object> values) {
        println "Update data:" + old + " to data:" + values
        tedisManager.getValueCommands().multiSet(customNamespace, values)
    }


    public void configure(PluginContext context) throws ReplicatorException, InterruptedException {
    }

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException {

    }

    public void release(PluginContext context) throws ReplicatorException, InterruptedException {
    }

	public String[] interest() {
		return ["*"] as String[] 
	}

	public Executor getExecutor() {
		return DIYExecutor.getInstance();
	}

}
