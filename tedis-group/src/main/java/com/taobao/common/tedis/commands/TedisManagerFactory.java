/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.group.TedisGroup;
import com.taobao.common.tedis.serializer.TedisSerializer;

public class TedisManagerFactory {
    private static ConcurrentHashMap<String, TedisManager> managers = new ConcurrentHashMap<String, TedisManager>();

    /**
     * 创建全局唯一的实例，如果已经存在，则返回存在的实例
     *
     * @param appName
     * @param version
     * @return
     */
    public static synchronized TedisManager create(String appName, String version) {
        String key = appName + "-" + version;
        TedisManager manager = managers.get(key);
        if (manager == null) {
            TedisGroup tedisGroup = new TedisGroup(appName, version);
            tedisGroup.init();
            manager = new DefaultTedisManager(tedisGroup);
            managers.put(key, manager);
        }
        return manager;
    }

    /**
     * 创建全局唯一的实例，如果已经存在，则返回存在的实例
     *
     * @param appName
     * @param version
     * @param serializer
     * @return
     */
    public static synchronized TedisManager create(String appName, String version, TedisSerializer<?> serializer) {
        String key = appName + "-" + version;
        DefaultTedisManager manager = (DefaultTedisManager) managers.get(key);
        if (manager == null) {
            TedisGroup tedisGroup = new TedisGroup(appName, version);
            tedisGroup.init();
            manager = new DefaultTedisManager(tedisGroup);
            managers.put(key, manager);
        }
        manager.setKeySerializer(serializer);
        manager.setValueSerializer(serializer);
        manager.setHashKeySerializer(serializer);
        return manager;
    }

    /**
     * 销毁指定的TedisManager。注意：在多线程环境下需要考虑是否还有其他线程还在使用，否则会导致问题。
     *
     * @param manager
     */
    public static synchronized void destroy(TedisManager manager) {
        Iterator<Entry<String, TedisManager>> it = managers.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().equals(manager)) {
                it.remove();
            }
        }
        manager.destroy();
    }

}
