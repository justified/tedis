/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.plugin;

import com.taobao.common.tedis.replicator.ReplicatorException;

public class PluginLoader {
    public static ReplicatorPlugin load(String name) throws ReplicatorException {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try {
            return (ReplicatorPlugin) Class.forName(name).newInstance();
        } catch (Exception e) {
            throw new PluginException(e);
        }
    }

    public static Class<?> loadClass(String name) throws ReplicatorException {
        if (name == null)
            throw new PluginException("Unable to load plugin with null name");
        try {
            return (Class<?>) Class.forName(name);
        } catch (Exception e) {
            throw new PluginException(e);
        }
    }
}
