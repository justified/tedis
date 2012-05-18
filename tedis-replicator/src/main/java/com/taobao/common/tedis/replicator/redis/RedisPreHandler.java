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

import com.taobao.common.tedis.core.TedisManager;

public interface RedisPreHandler {

	void beforeInsert(TedisManager tedisManager, String tableName, Map<String, Object> values);

    void beforeUpdate(TedisManager tedisManager, String tableName, Map<String, Object> old, Map<String, Object> values);

    void beforeDelete(TedisManager tedisManager, String tableName, Map<String, Object> old);
}
