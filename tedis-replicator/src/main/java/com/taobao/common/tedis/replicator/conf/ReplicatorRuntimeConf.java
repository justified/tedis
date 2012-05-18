/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.conf;

import java.io.File;

public class ReplicatorRuntimeConf {
	static public final String HOME_DIR = "replicator.home.dir";
	static public final String CONF_DIR = "replicator.conf.dir";
	static public final String HANDLER_DIR = "replicator.redis.handler.dir";
	static public final String DEFAULT_FOULDER_NAME = "/tedis-replicator";

	private static File replicatorHomeDir;
	private static File replicatorConfDir;
	private static File redisHandlerDir;

	private final File replicatorProperties;

	private ReplicatorRuntimeConf(String serviceName) {
		replicatorHomeDir = locateReplicatorHomeDir();
		replicatorConfDir = locateReplicatorConfDir();
		redisHandlerDir = locateRedisHandlerDir();

		replicatorProperties = new File(locateReplicatorConfDir(), serviceName + ".properties");

		if (!replicatorProperties.isFile() || !replicatorProperties.canRead()) {
			throw new ServerRuntimeException("Replicator static properties does not exist or is invalid: " + replicatorProperties);
		}
	}

	public static ReplicatorRuntimeConf getConfiguration(String serviceName) {
		return new ReplicatorRuntimeConf(serviceName);
	}

	public File getReplicatorHomeDir() {
		return replicatorHomeDir;
	}

	public File getReplicatorConfDir() {
		return replicatorConfDir;
	}

	public File getRedisHandlerDir() {
		return redisHandlerDir;
	}

	public File getReplicatorProperties() {
		return replicatorProperties;
	}

	public static File locateReplicatorHomeDir() {
		if (replicatorHomeDir == null) {
			String replicatorHome = System.getProperty(HOME_DIR);
			if (replicatorHome == null)
				replicatorHome = System.getProperty("user.home");
			replicatorHomeDir = new File(replicatorHome, DEFAULT_FOULDER_NAME);
			if (!replicatorHomeDir.isDirectory()) {
				throw new ServerRuntimeException("Replicator home does not exist or is invalid: " + replicatorHomeDir);
			}
		}
		return replicatorHomeDir;
	}

	public static File locateRedisHandlerDir() {
		if (redisHandlerDir == null) {
			String redisHandlerHome = System.getProperty(HANDLER_DIR);
			if (redisHandlerHome == null) {
				redisHandlerDir = new File(locateReplicatorHomeDir(), "groovy");
			} else {
				redisHandlerDir = new File(redisHandlerHome);
			}
			if (!redisHandlerDir.isDirectory()) {
				throw new ServerRuntimeException("Redis handler home does not exist or is invalid: " + redisHandlerDir);
			}
		}
		return redisHandlerDir;
	}

	public static File locateReplicatorConfDir() {
		if (replicatorConfDir == null) {
			// Configure replicator conf directory.
			String replicatorConf = System.getProperty(CONF_DIR);
			if (replicatorConf == null)
				replicatorConfDir = new File(locateReplicatorHomeDir(), "conf");
			else
				replicatorConfDir = new File(replicatorConf);
			if (!replicatorConfDir.isDirectory()) {
				throw new ServerRuntimeException("Replicator conf directory does not exist or is invalid: " + replicatorConfDir);
			}
		}
		return replicatorConfDir;
	}
}
