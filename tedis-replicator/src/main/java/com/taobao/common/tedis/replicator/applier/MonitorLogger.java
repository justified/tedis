/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.applier;

import java.io.File;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


public class MonitorLogger {
	
	public static final Logger DBSYNC_LOG = Logger.getLogger("DBSYNC_LOG");
	public static final Logger logger = DBSYNC_LOG;

	static private volatile boolean initOK = false;

	private static String getLogPath() {
		String userHome = System.getProperty("user.home");
		if (!userHome.endsWith(File.separator)) {
			userHome += File.separator;
		}
		String path = userHome + "logs" + File.separator + "dbsync" + File.separator;
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return path;
	}

	static {
		initTedisLog();
	}

	private static Appender buildAppender(String name, String fileName, String pattern) {
		DailyRollingFileAppender appender = new DailyRollingFileAppender();
		appender.setName(name);
		appender.setAppend(true);
		appender.setEncoding("GBK");
		appender.setLayout(new PatternLayout(pattern));
		appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
		appender.activateOptions();
		return appender;
	}

	static public void initTedisLog() {
		if (initOK)
			return;
		Appender statisticAppender = buildAppender("Tedis_Appender", "tedis-dbsync.log", "%m");
		DBSYNC_LOG.setAdditivity(false);
		DBSYNC_LOG.removeAllAppenders();
		DBSYNC_LOG.addAppender(statisticAppender);
		DBSYNC_LOG.setLevel(Level.INFO);
		initOK = true;
	}

}
