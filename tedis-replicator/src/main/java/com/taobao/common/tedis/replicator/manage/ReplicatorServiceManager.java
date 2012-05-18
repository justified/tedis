/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.replicator.ReplicatorException;
import com.taobao.common.tedis.replicator.ReplicatorProperties;
import com.taobao.common.tedis.replicator.conf.ConfigManager;
import com.taobao.common.tedis.replicator.conf.ConfigManager.HandlerPluginConfig;
import com.taobao.common.tedis.replicator.conf.ReplicatorConf;
import com.taobao.common.tedis.replicator.conf.ReplicatorRuntimeConf;
import com.taobao.common.tedis.replicator.redis.RedisHandlerLoader;
import com.taobao.common.tedis.replicator.util.PropertiesUtils;

public class ReplicatorServiceManager implements ServiceManager {
	private static final Logger logger = Logger.getLogger(ReplicatorServiceManager.class);
	private static final String DEFAULT_VERSION = "v0";

	private String version = DEFAULT_VERSION;
	private ConfigManager configManager;
	private ZookeeperManager zookeeperManager;
	private volatile boolean backup = false;
	private String zkAddress;

	private TreeMap<String, ReplicatorManager> replicators = new TreeMap<String, ReplicatorManager>();
	private Map<String, ReplicatorProperties> serviceConfigs = new TreeMap<String, ReplicatorProperties>();

	private static ServiceManager serviceManager = null;

	public static ServiceManager getInstance() {
		return getInstance(null);
	}

	public static synchronized ServiceManager getInstance(String address) {
		if (serviceManager == null) {
			serviceManager = new ReplicatorServiceManager(address);
		}
		return serviceManager;
	}

	private ReplicatorServiceManager() {
		this(null);
	}

	private ReplicatorServiceManager(String address) {
		this.configManager = new ConfigManager(version, this);
		if (address != null && !address.isEmpty()) {
			this.backup = true;
			this.zkAddress = address;
		}
		if (this.backup) {
			initZK();
		}
	}

	private void initZK() {
		this.zookeeperManager = new ZookeeperManager(this);
		this.zookeeperManager.setZkAddress(zkAddress);
		try {
			this.zookeeperManager.init();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void go() throws Exception {
		loadServiceConfig();

		for (String service : serviceConfigs.keySet()) {
			startService(service);
		}
	}

	@Override
	public void restart() throws Exception {
		for (String service : serviceConfigs.keySet()) {
			stopService(service);
		}
		go();
	}

	private void loadServiceConfig() throws ReplicatorException {
		serviceConfigs.clear();
		List<HandlerPluginConfig> list = this.configManager.getPluginList();
		for (HandlerPluginConfig config : list) {
			String serviceName = config.getPlugin();
			ReplicatorProperties properties = ReplicatorManager.getConfigurationProperties(serviceName);
			serviceConfigs.put(serviceName, properties);
		}
	}

	@Override
	public Map<String, String> redisHandlers() {
		List<String> extensionNames = new ArrayList<String>();
		for (String service : serviceConfigs.keySet()) {
			ReplicatorManager manager = replicators.get(service);
			if (manager != null) {
				extensionNames.addAll(manager.getExtensionNames());
			}
		}
		Map<String, String> result = new HashMap<String, String>();
		for (String extension : extensionNames) {
			result.put(extension, RedisHandlerLoader.content(extension));
		}
		return result;
	}

	@Override
	public String redisHandlerContent(String handler) {
		return RedisHandlerLoader.content(handler);
	}

	@Override
	public boolean updateRedisHandler(String name, String content) {
		return RedisHandlerLoader.update(name, content);
	}

	@Override
	public void updatePropertyFile(String serviceName, Map<String, String> props) {
		try {
			ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf.getConfiguration(serviceName);
			PropertiesUtils.setProperty(runtimeConf.getReplicatorProperties().getAbsolutePath(), props);
		} catch (Exception e) {
			logger.error("Update property file failed.", e);
		}
	}

	@Override
	public List<Map<String, String>> services() throws Exception {
		if (!tryLock()) {
			return null;
		}

		List<Map<String, String>> services = new ArrayList<Map<String, String>>();

		for (String name : serviceConfigs.keySet()) {
			Map<String, String> info = new TreeMap<String, String>();
			info.put("name", name);

			if (replicators.get(name) != null) {
				info.put("started", "true");
			} else {
				info.put("started", "false");
			}
			services.add(info);
		}
		return services;
	}

	@Override
	public List<Map<String, String>> status() throws Exception {
		if (!tryLock()) {
			return null;
		}

		List<Map<String, String>> status = new ArrayList<Map<String, String>>();

		for (String name : serviceConfigs.keySet()) {
			Map<String, String> info = new TreeMap<String, String>();
			ReplicatorManager manager = replicators.get(name);
			info.put("name", name);
			if (manager != null) {
				info.put("started", "true");
				info.putAll(manager.status());
			} else {
				info.put("started", "false");
			}
			status.add(info);
		}
		return status;
	}

	@Override
	public Map<String, String> status(String serviceName) throws Exception {
		if (!tryLock()) {
			return null;
		}
		if (!serviceConfigs.keySet().contains(serviceName)) {
			return null;
		} else {
			Map<String, String> info = new TreeMap<String, String>();
			ReplicatorManager manager = replicators.get(serviceName);
			info.put("name", serviceName);
			if (manager != null) {
				info.put("started", "true");
				info.putAll(manager.status());
			} else {
				info.put("started", "false");
			}
			return info;
		}
	}

	private boolean tryLock() {
		boolean result = !backup || (backup && zookeeperManager.tryLock());
		if (backup && result == false) {
			logger.info("Didn't get the lock, may be other instance has allready running.");
		}
		return result;
	}

	@Override
	public boolean startService(String serviceName) throws Exception {
		if (!tryLock()) {
			return false;
		}
		if (!serviceConfigs.keySet().contains(serviceName)) {
			throw new Exception("Unknown replication service name: " + serviceName);
		} else if (replicators.get(serviceName) == null) {
			startReplicationService(serviceConfigs.get(serviceName));
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean stopService(String serviceName) throws Exception {
		if (!tryLock()) {
			return false;
		}
		if (!serviceConfigs.keySet().contains(serviceName)) {
			throw new Exception("Unknown replication service name: " + serviceName);
		} else if (replicators.get(serviceName) == null) {
			return false;
		} else {
			stopReplicationService(serviceName);
			return true;
		}
	}

	private void startReplicationService(ReplicatorProperties replProps) throws ReplicatorException {
		String serviceName = replProps.getString(ReplicatorConf.SERVICE_NAME);
		ReplicatorManager rm = null;
		try {
			rm = new ReplicatorManager(serviceName);
			rm.start();
			replicators.put(serviceName, rm);
			logger.info(String.format("Replication service '%s' started successfully", serviceName));
		} catch (Exception e) {
			logger.error(String.format("Unable to start replication service '%s'", serviceName), e);
		}
	}

	private void stopReplicationService(String serviceName) throws Exception {
		logger.info("Stopping replication service: name=" + serviceName);

		ReplicatorManager manager = replicators.get(serviceName);
		try {
			manager.offline();
		} catch (Exception e) {
			logger.warn("Could not place service in offline state: " + e.getMessage());
		}
		manager.stop();
		replicators.remove(serviceName);
		logger.info("Replication service stopped successfully: name=" + serviceName);
	}

	@Override
	public boolean online(String serviceName) throws Exception {
		if (!tryLock()) {
			return false;
		}
		if (!serviceConfigs.keySet().contains(serviceName)) {
			throw new Exception("Unknown replication service name: " + serviceName);
		} else if (replicators.get(serviceName) == null) {
			return false;
		} else {
			logger.info("Online replication service: name=" + serviceName);
			ReplicatorManager manager = replicators.get(serviceName);
			if (manager.getState().contains("")) {

			}
			try {
				manager.online();
			} catch (Exception e) {
				logger.error("Could not place service in online state: " + e.getMessage());
				return false;
			}
			return true;
		}
	}

	@Override
	public boolean offline(String serviceName) throws Exception {
		if (!tryLock()) {
			return false;
		}
		if (!serviceConfigs.keySet().contains(serviceName)) {
			throw new Exception("Unknown replication service name: " + serviceName);
		} else if (replicators.get(serviceName) == null) {
			return false;
		} else {
			logger.info("Offline replication service: name=" + serviceName);
			ReplicatorManager manager = replicators.get(serviceName);
			try {
				manager.offline();
			} catch (Exception e) {
				logger.error("Could not place service in offline state: " + e.getMessage());
				return false;
			}
			return true;
		}
	}

	@Override
	public boolean configure(String serviceName, HashMap<String, String> props) throws Exception {
		if (!serviceConfigs.keySet().contains(serviceName)) {
			throw new Exception("Unknown replication service name: " + serviceName);
		} else if (replicators.get(serviceName) == null) {
			return false;
		} else {
			logger.info("Configure replication service: name=" + serviceName + "; props=" + props);
			try {
				ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf.getConfiguration(serviceName);
				PropertiesUtils.setProperty(runtimeConf.getReplicatorProperties().getAbsolutePath(), props);
			} catch (Exception e) {
				logger.error("Could configure service cause:" + e.getMessage());
				return false;
			}
		}
		return true;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
