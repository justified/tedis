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

/**
 * 一个插件的生命周期如下：
 * <p>
 * <ol>
 * <li>Instantiate plug-in from class name</li>
 * <li>Call setters on plug-in instance and load property names</li>
 * <li>Call configure() to signal configuration is complete</li>
 * <li>Call prepare() to create resources for operation</li>
 * <li>(Type-specific plug-in method calls)</li>
 * <li>Call release() to free resources</li>
 * </ol>
 */
public interface ReplicatorPlugin {

    public void configure(PluginContext context) throws ReplicatorException, InterruptedException;

    public void prepare(PluginContext context) throws ReplicatorException, InterruptedException;

    public void release(PluginContext context) throws ReplicatorException, InterruptedException;
}
