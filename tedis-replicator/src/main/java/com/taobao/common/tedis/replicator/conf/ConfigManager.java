/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.manage.ReplicatorServiceManager;
import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;

public class ConfigManager {
    private static Logger logger = Logger.getLogger(ConfigManager.class);

    private static String defaultConfKey = "com.taobao.common.tedis.config.replicator.";
    private DiamondManager diamondManager;
    private int timeout = 3000;
    private ManagerListenerImpl managerListener = new ManagerListenerImpl();
    private ReplicatorServiceManager serviceManager;

    private List<HandlerPluginConfig> pluginList = new CopyOnWriteArrayList<HandlerPluginConfig>();

    public ConfigManager(String version, ReplicatorServiceManager serviceManager) {
        this.serviceManager = serviceManager;
        diamondManager = new DefaultDiamondManager(defaultConfKey + version, managerListener);
        String configString = diamondManager.getConfigureInfomation(timeout);
        parseConfig(configString);
    }

    private List<HandlerPluginConfig> parseConfig(String config) {
        pluginList.clear();
        if (config != null && !config.isEmpty()) {
            String[] plugins = config.split(";");
            if (plugins != null) {
                for(String p : plugins) {
                    if (!p.contains("=")) {
                        logger.warn("Cann't parse the handler config:" + p);
                        continue;
                    }
                    int index = p.indexOf("=");
                    String name = p.substring(0, index);
                    String value = p.substring(index + 1);
                    HandlerPluginConfig hpc = new HandlerPluginConfig(name);
                    if (value != null && !value.isEmpty()) {
                        for(String v : value.split(",")) {
                            hpc.addHandler(v);
                        }
                    }
                    pluginList.add(hpc);
                }
            }
        }
        return null;
    }

    public List<HandlerPluginConfig> getPluginList() {
        return pluginList;
    }

    public class HandlerPluginConfig {
        private String plugin;
        private List<String> handlers = new ArrayList<String>();
        public HandlerPluginConfig(String plugin) {
            this.plugin = plugin;
        }

        public void addHandler(String handler) {
            handlers.add(handler);
        }

        public String getPlugin() {
            return plugin;
        }

        public void setPlugin(String plugin) {
            this.plugin = plugin;
        }

        public List<String> getHandlers() {
            return handlers;
        }

        @Override
        public String toString() {
            return "HandlerPluginConfig [plugin=" + plugin + ",handlers=" + handlers + "]";
        }
    }

    class ManagerListenerImpl implements ManagerListener {
        @Override
        public Executor getExecutor() {
            return DIYExecutor.getInstance();
        }

        @Override
        public void receiveConfigInfo(String string) {
            logger.info("Config changes£º" + string);
            parseConfig(string);
            try {
                serviceManager.restart();
            } catch (Exception e) {
                logger.error("Restart service error:", e);
            }
        }
    }

}
