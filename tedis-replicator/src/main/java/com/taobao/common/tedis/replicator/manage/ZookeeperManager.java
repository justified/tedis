/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.manage;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.taobao.common.tedis.dislock.CallBack;
import com.taobao.common.tedis.dislock.Lock;
import com.taobao.common.tedis.dislock.ZKLockFactory;

public class ZookeeperManager {
	private static final Logger logger = Logger.getLogger(ZookeeperManager.class);
	private static final String DEFAULT_APPNAME = "tedis";
	private static final String DEFAULT_LOCKPATH = "replicator-lock";

	private Lock lock;
	private String zkAddress;
	private String zkTimeout = "5000";
	private String appName = DEFAULT_APPNAME;

	private ReplicatorServiceManager serviceManager;
	
	private volatile boolean locked = false;

	public ZookeeperManager(ReplicatorServiceManager serviceManager) {
		this.serviceManager = serviceManager;
	}

	public void init() throws Exception {
		if (zkAddress == null || zkAddress.isEmpty()) {
			throw new Exception("Zookeeper server address cannot be null or empty.");
		}
		try {
			ZKLockFactory zkLockFactory = new ZKLockFactory();
			Properties properties = new Properties();
			properties.put("zkAddress", zkAddress);
			properties.put("zkTimeout", zkTimeout);
			properties.put("appName", appName);
			zkLockFactory.setProperties(properties);
			zkLockFactory.init();
			lock = zkLockFactory.getLock(DEFAULT_LOCKPATH);
			lock.registerListener(new ReplicatorCallback(), null);
			if (logger.isDebugEnabled()) {
				logger.debug("Zookeeper init successful! ");
			}
			this.locked = tryLock();
		} catch (Exception e) {
			logger.error("Zookeeper init failed.", e);
			throw new Exception("Zookeeper init failed", e);
		}
	}

	public boolean tryLock() {
		return lock != null && lock.tryLock();
	}

	class ReplicatorCallback implements CallBack {
		@Override
		public void doTask(String path, Object ctx) {
			if (tryLock()) {
				try {
					if (!locked) {
						serviceManager.restart();
						locked = true;
					}
				} catch (Exception e) {
					logger.error("Restart replication service failed.", e);
					throw new RuntimeException("Restart replication service failed.", e);
				}
			}
		}
	}

	public void release() {
		if (lock != null && lock.isOwner()) {
			lock.unlock();
			lock = null;
		}
		this.locked = false;
	}

	public String getZkAddress() {
		return zkAddress;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	public String getZkTimeout() {
		return zkTimeout;
	}

	public void setZkTimeout(String zkTimeout) {
		this.zkTimeout = zkTimeout;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

}
