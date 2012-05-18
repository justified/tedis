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
import com.taobao.common.tedis.replicator.ReplicatorProperties;

public class PluginSpecification {
    private final String prefix;
    private final String name;
    private final Class<?> pluginClass;
    private final ReplicatorProperties properties;

    public PluginSpecification(String prefix, String name, Class<?> pluginClass, ReplicatorProperties properties) {
        this.prefix = prefix;
        this.name = name;
        this.pluginClass = pluginClass;
        this.properties = properties;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getName() {
        return name;
    }

    public Class<?> getPluginClass() {
        return pluginClass;
    }

    public ReplicatorProperties getProperties() {
        return properties;
    }

    public ReplicatorPlugin instantiate(int id) throws ReplicatorException {
        ReplicatorPlugin plugin = PluginLoader.load(pluginClass.getName());
        properties.applyProperties(plugin);
        return plugin;
    }
}
